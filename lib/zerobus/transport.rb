# frozen_string_literal: true
require 'uri'
require 'typhoeus'
module Zerobus
  # Typhoeus builds the request and streams responses; its Ethon backend drives
  # libcurl incrementally so an empty upload can pause while ACKs keep arriving.
  class Transport
    READ_PAUSE = 0x10000001 # CURL_READFUNC_PAUSE, from curl.h
    READ_ABORT = 0x10000000
    class Driver < Ethon::Multi
      # Ethon#perform runs until completion. A bidi RPC needs to return control
      # between multi_perform calls to admit more records and enforce deadlines.
      def tick = run
    end

    def initialize(host:, headers:, method:, timeout:)
      unless (Ethon::Curl.version_info[:features] & Ethon::Curl::VERSION_HTTP2).positive?
        raise NonRetriableException, 'libcurl must be built with HTTP/2 support'
      end
      @lock = Mutex.new
      @messages, @upload = [], []
      @upload_cursor = 0
      @decoder = Protocol::Decoder.new
      path = method.start_with?('/') ? method : "/databricks.zerobus.Zerobus/#{method}"
      url = host.sub(%r{/$}, '') + path
      @request = Typhoeus::Request.new(url, method: :post, http_version: :httpv2_prior_knowledge,
        connecttimeout_ms: [(timeout * 1000).ceil, 1].max, timeout_ms: 0,
        followlocation: false, suppress_connect_headers: true, ssl_verifypeer: true, ssl_verifyhost: 2,
        headers: {'content-type' => 'application/grpc', 'te' => 'trailers',
                  'grpc-accept-encoding' => 'identity', 'expect' => ''}.merge(headers))
      @request.on_body do |chunk, _response|
        begin
          @decoder.feed(chunk) { |message| @messages << message }
          nil
        rescue StandardError => error
          @error ||= error
          :abort
        end
      end
      @request.on_complete do |response|
        @ended = true
        begin
          @decoder.finish!
          unless response.return_code == :ok
            @error ||= ZerobusException.new("libcurl transport failed: #{response.return_code}")
          end
          @error ||= NonRetriableException.new('gRPC response missing grpc-status trailers') unless @grpc_status
        rescue StandardError => error
          @error ||= error
        end
      end
      @factory = Typhoeus::EasyFactory.new(@request)
      @easy = @factory.get
      # Unknown-length POST body, fed by our read callback rather than POSTFIELDS.
      @easy.upload = true
      @easy.customrequest = 'POST'
      @easy.infilesize_large = -1
      @easy.readfunction { |pointer, size, count, _| read_upload(pointer, size * count) }
      @easy.headerfunction do |pointer, size, count, _|
        begin
          line = pointer.read_string(size * count)
          @easy.response_headers << line
          receive_header(line)
          size * count
        rescue StandardError => error
          @error ||= error
          0
        end
      end
      @multi = Driver.new
      @multi.add(@easy)
    rescue StandardError
      close
      raise
    end

    def send_message(message)
      @lock.synchronize do
        raise @error if @error
        raise ZerobusException, 'gRPC stream ended' if @closed || @ended
        @upload << Protocol.frame(message)
      end
    end
    def next_message(timeout:)
      @lock.synchronize do
        deadline = monotonic + timeout
        loop do
          return @messages.shift unless @messages.empty?
          raise @error if @error
          raise ClosedStreamError, 'transport closed' if @closed
          return nil if @ended
          if @paused && (!@upload.empty? || @finish_request)
            @paused = false
            result = @easy.pause(Ethon::Curl::PAUSE_CONT)
            raise ZerobusException, "libcurl resume failed: #{result}" unless result == :ok
          end
          @multi.tick
          next unless @messages.empty?
          raise @error if @error
          return nil if @ended
          remaining = deadline - monotonic
          return nil if remaining <= 0
          # libcurl handles socket readiness, TLS, HPACK and HTTP/2 flow control.
          @multi.poll([(remaining * 1000).ceil, 10].min)
        end
      end
    end
    def finish_request
      @lock.synchronize { @finish_request = true }
    end
    def completed?
      @lock.synchronize do
        raise @error if @error
        @ended && @grpc_status == 0
      end
    end
    def close
      return unless @lock
      @lock.synchronize do
        return if @closed
        @closed = true
        @multi&.cleanup
        @factory&.release_easy(@easy) if @easy
        @easy = nil
        @upload&.clear
      end
    end

    private

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    def read_upload(pointer, capacity)
      return READ_ABORT if @closed || @error
      if @upload.empty?
        return 0 if @finish_request
        @paused = true
        return READ_PAUSE
      end
      frame = @upload.first
      chunk = frame.byteslice(@upload_cursor, capacity)
      pointer.put_bytes(0, chunk)
      @upload_cursor += chunk.bytesize
      if @upload_cursor == frame.bytesize
        @upload.shift
        @upload_cursor = 0
      end
      chunk.bytesize
    rescue StandardError => error
      @error ||= error
      READ_ABORT
    end
    def receive_header(line)
      if line.start_with?('HTTP/')
        version, code, = line.split(' ')
        raise NonRetriableException, 'server did not negotiate HTTP/2' unless version == 'HTTP/2'
        status = Integer(code)
        if status != 200
          raise RpcError.new([429, 502, 503, 504].include?(status) ? 14 : 7, "HTTP #{status}")
        end
      elsif line.include?(':')
        name, value = line.split(':', 2)
        case name.downcase
        when 'grpc-message' then @grpc_message = URI::DEFAULT_PARSER.unescape(value.strip)
        when 'grpc-status'
          @grpc_status = Integer(value.strip)
        end
      end
      if line == "\r\n" && @grpc_status && @grpc_status != 0
        @error ||= RpcError.new(@grpc_status, @grpc_message || '')
      end
    end
  end
end
