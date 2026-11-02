# frozen_string_literal: true
module Zerobus
  # A small Ruby-thread future. Work begins immediately; value/wait propagates
  # the original exception. A wait timeout does not cancel ingestion.
  class Future
    def initialize(&block)
      @thread = Thread.new(&block)
      @thread.report_on_exception = false
    end
    def value(timeout: nil)
      raise TimeoutError, 'future wait timed out' unless @thread.join(timeout)
      @thread.value
    end
    alias wait value
    def complete? = !@thread.alive?
  end

  module Async
    class ZerobusStream
      attr_reader :inner
      def initialize(inner) = @inner = inner
      [:ingest_record_offset, :ingest_records_offset, :ingest_record, :ingest_record_nowait,
       :ingest_records_nowait, :wait_for_offset, :flush, :close, :get_unacked_records,
       :get_unacked_batches].each do |method|
        define_method(method) do |*args, **kwargs|
          Future.new { inner.public_send(method, *args, **kwargs) }
        end
      end
      def stream_id = inner.stream_id
      def resume_watermark = inner.resume_watermark
      def get_state = inner.get_state
      def is_closed = inner.is_closed
    end
    class PersistentStreamBuilder
      def initialize(inner) = @inner = inner
      def build = Future.new { ZerobusStream.new(@inner.build) }
      def resume(stream_id) = Future.new { ZerobusStream.new(@inner.resume(stream_id)) }
      def method_missing(method, *args, **kwargs, &block)
        return super unless @inner.respond_to?(method)
        @inner.public_send(method, *args, **kwargs, &block)
        self
      end
      def respond_to_missing?(method, include_private = false) = @inner.respond_to?(method, include_private) || super
    end
    class ZerobusSdk
      def initialize(**kwargs) = @inner = ::Zerobus::ZerobusSdk.new(**kwargs)
      def create_stream(**kwargs) = Future.new { ZerobusStream.new(@inner.create_stream(**kwargs)) }
      def persistent_stream_builder = PersistentStreamBuilder.new(@inner.persistent_stream_builder)
      def recreate_stream(old_stream) = Future.new { ZerobusStream.new(@inner.recreate_stream(old_stream.inner)) }
      def retire_stream(**kwargs) = Future.new { @inner.retire_stream(**kwargs) }
    end
  end
end
