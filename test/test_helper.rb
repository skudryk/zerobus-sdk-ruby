# frozen_string_literal: true
require 'minitest/autorun'
require 'timeout'
require 'socket'
require 'zerobus'
require 'http/2'
require 'minitest/mock'

module TestSupport
  P = Zerobus::Protocol
  def props = Zerobus::TableProperties.new('catalog.schema.events')
  def headers = -> { {'authorization' => 'Bearer test'} }
  def options(**values)
    Zerobus::StreamConfigurationOptions.new(recovery_timeout_ms: 2_000, recovery_backoff_ms: 1,
                                             flush_timeout_ms: 2_000, **values)
  end
  def eventually
    Timeout.timeout(3) { Thread.pass until yield }
  end
  def sdk_for(factory)
    Zerobus::ZerobusSdk.new(host: 'http://localhost:8080', unity_catalog_url: 'https://workspace.example',
                            transport_factory: factory)
  end
  def open_stream(sdk, **args)
    sdk.create_stream(table_properties: props, headers_provider: headers, options: options, **args)
  end

  class FakeTransport
    attr_reader :sent
    def initialize(persistent: false, watermark: nil, auto_ack: true, &on_send)
      @persistent, @watermark, @auto_ack, @on_send = persistent, watermark, auto_ack, on_send
      @sent = Queue.new
      @incoming = Queue.new
      @opened = false
      @closed = false
    end
    def send_message(message)
      raise IOError, 'closed' if @closed
      @sent << message
      unless @opened
        @opened = true
        if @watermark
          @incoming << P.bytes(2, @watermark == -1 ? ''.b : P.integer(1, @watermark))
        else
          @incoming << P.bytes(1, P.bytes(1, 'stream-123'))
        end
        return
      end
      @on_send&.call(self, message)
      if @auto_ack
        outer = P.fields(message)
        payload = outer.fetch(@persistent ? (outer.key?(3) ? 3 : 4) : (outer.key?(2) ? 2 : 3)).first
        ack(P.fields(payload).fetch(1).first)
      end
    end
    def ack(offset) = @incoming << P.bytes(@persistent ? 3 : 2, P.integer(1, offset))
    def inject(value) = @incoming << value
    def next_message(timeout:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      loop do
        raise IOError, 'closed' if @closed
        begin
          value = @incoming.pop(true)
          raise value if value.is_a?(Exception)
          return value
        rescue ThreadError
          return nil if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          sleep 0.001
        end
      end
    end
    def close = @closed = true
  end
end
