# frozen_string_literal: true
require_relative 'test_helper'
class TransportTest < Minitest::Test
  include TestSupport

  # A real HTTP/2 TCP peer tests framing, headers, fragmentation and flow control.
  def with_server(handler)
    listener = TCPServer.new('127.0.0.1', 0)
    errors = Queue.new
    sockets = Queue.new
    server = Thread.new do
      socket = listener.accept
      sockets << socket
      connection = HTTP2::Server.new
      # http-2 1.3 rejects even the legal gRPC `te: trailers` header.
      connection.instance_variable_get(:@decompressor).accept_invalid_headers!
      connection.on(:frame) { |bytes| socket.write(bytes) }
      connection.on(:stream) do |stream|
        decoder = P::Decoder.new
        metadata = {}
        stream.on(:headers) { |values| metadata.merge!(values.to_h) }
        stream.on(:data) do |data|
          decoder.feed(data) { |message| handler.call(stream, message, metadata) }
        end
        stream.on(:half_close) { handler.call(stream, :end, metadata) }
      end
      loop { connection << socket.readpartial(64 * 1024) }
    rescue EOFError, IOError, Errno::ECONNRESET
      nil
    rescue StandardError => error
      errors << error
    end
    yield "http://127.0.0.1:#{listener.addr[1]}", errors, server, sockets
  ensure
    listener&.close
    sockets&.pop(true)&.close rescue nil
    server&.join(1)
  end

  def run_server(handler)
    with_server(handler) do |host, errors, _server, _sockets|
      yield host
      raise errors.pop unless errors.empty?
    end
  end

  def test_real_bidirectional_stream_and_large_payload_flow_control
    received = Queue.new
    handler = lambda do |stream, message, metadata|
      next if message == :end
      received << [message, metadata.dup]
      top = P.fields(message)
      if top.key?(1)
        stream.headers({':status' => '200', 'content-type' => 'application/grpc'}, end_stream: false)
        frame = P.frame(P.bytes(1, P.bytes(1, 'real-stream')))
        frame.each_byte { |byte| stream.data(byte.chr.b, end_stream: false) }
      else
        payload = P.fields(top.fetch(2).first)
        stream.data(P.frame(P.bytes(2, P.integer(1, payload[1].first))), end_stream: false)
      end
    end
    run_server(handler) do |host|
      sdk = Zerobus::ZerobusSdk.new(host: host, unity_catalog_url: 'https://workspace.example', application_name: 'test/1')
      stream = open_stream(sdk)
      begin
        assert_equal 'real-stream', stream.stream_id
        3.times { stream.ingest_record_offset({'text' => 'a' * 200_000}) }
        stream.flush
        assert_equal 4, received.size
        _, metadata = received.pop
        assert_equal '/databricks.zerobus.Zerobus/EphemeralStream', metadata[':path']
        assert_equal 'Bearer test', metadata['authorization']
        assert_equal props.table_name, metadata['x-databricks-zerobus-table-name']
        assert_includes metadata['user-agent'], 'test/1'
      ensure
        stream.close
      end
    end
  end

  def test_grpc_error_trailers
    handler = lambda do |stream, message, _|
      next if message == :end
      stream.headers({':status' => '200', 'grpc-status' => '7', 'grpc-message' => 'access%20denied'}, end_stream: true)
    end
    run_server(handler) do |host|
      sdk = Zerobus::ZerobusSdk.new(host: host, unity_catalog_url: 'https://workspace.example')
      error = assert_raises(Zerobus::RpcError) { open_stream(sdk) }
      assert_equal 7, error.code
      assert_includes error.message, 'access denied'
    end
  end

  def test_retire_stream_sends_unary_request_and_checks_trailers
    received = Queue.new
    handler = lambda do |stream, message, metadata|
      if message == :end
        stream.headers({':status' => '200', 'content-type' => 'application/grpc'}, end_stream: false)
        stream.data(P.frame(''.b), end_stream: false)
        stream.headers({'grpc-status' => '0'}, end_stream: true)
      else
        received << [message, metadata]
      end
    end
    run_server(handler) do |host|
      sdk = Zerobus::ZerobusSdk.new(host: host, unity_catalog_url: 'https://workspace.example')
      sdk.retire_stream(table_name: props.table_name, stream_id: 'retired', headers_provider: headers)
      message, metadata = received.pop
      assert_equal '/databricks.zerobus.Zerobus/RetireStream', metadata[':path']
      assert_equal props.table_name, P.fields(message)[1].first
      assert_equal 'retired', P.fields(message)[2].first
    end
  end
end
