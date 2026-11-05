# frozen_string_literal: true
module Zerobus
  module Async
    class ExecutorShutdownError < NonRetriableException; end

    # An SDK and its streams share this fixed pool. SizedQueue applies
    # backpressure in the caller before another operation can be queued.
    class Executor
      DEFAULT_WORKER_COUNT = 4
      DEFAULT_QUEUE_CAPACITY = 1000
      DEFAULT_MUTEX = Mutex.new

      def self.default
        DEFAULT_MUTEX.synchronize { @default ||= new }
      end

      def initialize(worker_count: DEFAULT_WORKER_COUNT, queue_capacity: DEFAULT_QUEUE_CAPACITY)
        {worker_count: worker_count, queue_capacity: queue_capacity}.each do |name, value|
          raise ArgumentError, "#{name} must be a positive integer" unless value.is_a?(Integer) && value.positive?
        end
        @queue = SizedQueue.new(queue_capacity)
        @workers = []
        worker_count.times do
          worker = Thread.new do
            while (job = @queue.pop)
              job.call
            end
          end
          worker.report_on_exception = false
          @workers << worker
        end
      rescue ThreadError
        shutdown
        raise
      end

      def post(&job)
        raise ArgumentError, 'work block is required' unless job
        raise ExecutorShutdownError, 'async executor is shut down' if @queue.closed?

        # A nested future can use its already admitted worker. Enqueueing and
        # waiting from that worker could otherwise deadlock a saturated pool.
        if @workers.include?(Thread.current)
          job.call
        else
          @queue.push(job)
        end
        nil
      rescue ClosedQueueError
        raise ExecutorShutdownError, 'async executor is shut down'
      end

      def shutdown(wait: true)
        @queue.close
        # A worker cannot join itself. It will exit after draining accepted work.
        @workers.each(&:join) if wait && !@workers.include?(Thread.current)
        nil
      end
    end
  end

  # Futures store results and wake waiters; execution belongs to a bounded pool.
  # A timeout bounds only the wait and does not cancel the operation.
  class Future
    def initialize(executor: Async::Executor.default, &block)
      raise ArgumentError, 'work block is required' unless block

      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @complete = false
      executor.post { execute(&block) }
    end

    def value(timeout: nil)
      deadline = monotonic + timeout if timeout
      @mutex.synchronize do
        until @complete
          remaining = deadline - monotonic if deadline
          raise TimeoutError, 'future wait timed out' if remaining && remaining <= 0
          @condition.wait(@mutex, remaining)
        end
        raise @error if @error
        @result
      end
    end

    alias wait value

    def complete? = @mutex.synchronize { @complete }

    private

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def execute
      result = yield
    rescue Exception => error # Store task failures so they cannot terminate a pool worker.
      failure = error
    ensure
      @mutex.synchronize do
        @result, @error = result, failure
        @complete = true
        @condition.broadcast
      end
    end
  end

  module Async
    class ZerobusStream
      attr_reader :inner

      def initialize(inner, executor: Executor.default)
        @inner, @executor = inner, executor
      end

      [:ingest_record_offset, :ingest_records_offset, :ingest_record, :ingest_record_nowait,
       :ingest_records_nowait, :wait_for_offset, :flush, :close, :get_unacked_records,
       :get_unacked_batches].each do |method|
        define_method(method) do |*args, **kwargs|
          Future.new(executor: @executor) { inner.public_send(method, *args, **kwargs) }
        end
      end

      def stream_id = inner.stream_id
      def resume_watermark = inner.resume_watermark
      def get_state = inner.get_state
      def is_closed = inner.is_closed
    end

    class PersistentStreamBuilder
      def initialize(inner, executor: Executor.default)
        @inner, @executor = inner, executor
      end

      def build
        Future.new(executor: @executor) { ZerobusStream.new(@inner.build, executor: @executor) }
      end

      def resume(stream_id)
        Future.new(executor: @executor) { ZerobusStream.new(@inner.resume(stream_id), executor: @executor) }
      end

      def method_missing(method, *args, **kwargs, &block)
        return super unless @inner.respond_to?(method)
        @inner.public_send(method, *args, **kwargs, &block)
        self
      end

      def respond_to_missing?(method, include_private = false) = @inner.respond_to?(method, include_private) || super
    end

    class ZerobusSdk
      def initialize(worker_count: Executor::DEFAULT_WORKER_COUNT, queue_capacity: Executor::DEFAULT_QUEUE_CAPACITY, **kwargs)
        @inner = ::Zerobus::ZerobusSdk.new(**kwargs)
        @executor = Executor.new(worker_count: worker_count, queue_capacity: queue_capacity)
      end

      def create_stream(**kwargs)
        Future.new(executor: @executor) { ZerobusStream.new(@inner.create_stream(**kwargs), executor: @executor) }
      end

      def persistent_stream_builder = PersistentStreamBuilder.new(@inner.persistent_stream_builder, executor: @executor)

      def recreate_stream(old_stream)
        Future.new(executor: @executor) { ZerobusStream.new(@inner.recreate_stream(old_stream.inner), executor: @executor) }
      end

      def retire_stream(**kwargs) = Future.new(executor: @executor) { @inner.retire_stream(**kwargs) }

      # Close streams before shutdown; accepted operations drain before workers exit.
      def shutdown(wait: true) = @executor.shutdown(wait: wait)
    end
  end
end
