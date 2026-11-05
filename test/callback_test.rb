# frozen_string_literal: true
require_relative 'test_helper'

class CallbackTest < Minitest::Test
  include TestSupport

  class SlowCallback < Zerobus::AckCallback
    attr_accessor :stream
    attr_reader :started, :delivered

    def initialize
      @started, @delivered, @release = Queue.new, Queue.new, Queue.new
    end

    def on_ack(offset) = deliver(:ack, offset)
    def on_error(offset, _) = deliver(:error, offset)
    def release(count = 1) = count.times { @release << true }

    private

    def deliver(kind, offset)
      stream.get_state # Callbacks must be able to acquire the stream mutex.
      @started << [kind, offset]
      @release.pop
      @delivered << [kind, offset]
    end
  end

  def teardown
    @ack_release&.push(true)
    @callback&.release(20)
    @stream&.close rescue nil
    @producer&.join(2)
    @stream&.instance_variable_get(:@callback_thread)&.join(2)
  end

  def open_with_callback(transport: FakeTransport.new, **settings)
    @callback = SlowCallback.new
    @stream = open_stream(sdk_for(->(**_) { transport }), options: options(
      ack_callback: @callback, callback_queue_capacity: 2, **settings))
    @callback.stream = @stream
    @stream
  end

  def started = Timeout.timeout(2) { @callback.started.pop }

  def test_slow_callbacks_backpressure_ingestion_after_ack_releases_record_capacity
    stream = open_with_callback(max_inflight_records: 1)
    assert_equal 0, stream.ingest_record_offset('{}')
    stream.flush
    assert_equal [:ack, 0], started
    assert_equal 1, stream.ingest_record_offset('{}')
    stream.flush
    queue = stream.instance_variable_get(:@callbacks)
    assert_instance_of SizedQueue, queue
    assert_equal 1, queue.size

    result = Queue.new
    @producer = Thread.new { result << stream.ingest_record_offset('{}') }
    eventually { @producer.status == 'sleep' }
    assert result.empty?, 'ACKs released callback capacity before callback completion'
    stream.flush # Durability and state queries must remain responsive.
    assert_equal :open, stream.get_state

    @callback.release
    assert_equal [:ack, 1], started
    assert @producer.join(2), 'callback completion did not wake the producer'
    assert_equal 2, result.pop(true)
    stream.flush
    assert_operator queue.size, :<=, queue.max
    @callback.release(2)
    stream.close
    assert_equal [[:ack, 0], [:ack, 1], [:ack, 2]], 3.times.map { @callback.delivered.pop(true) }
    assert @callback.delivered.empty?
  end

  def test_cumulative_ack_reserves_one_callback_slot_per_batch_before_ack
    transport = FakeTransport.new(auto_ack: false)
    stream = open_with_callback(transport: transport, max_inflight_records: 100)
    assert_equal 0, stream.ingest_records_offset(['{}'] * 10)
    assert_equal 1, stream.ingest_record_offset('{}')
    result = Queue.new
    @producer = Thread.new { result << stream.ingest_record_offset('{}') }
    eventually { @producer.status == 'sleep' }
    assert result.empty?
    eventually { transport.sent.size == 3 }
    transport.ack(1)
    stream.flush
    assert_equal [:ack, 0], started
    assert result.empty?, 'cumulative ACK bypassed callback backpressure'
    @callback.release
    assert @producer.join(2)
    assert_equal 2, result.pop(true)
    eventually { transport.sent.size == 4 }
    transport.ack(2)
    @callback.release(2)
    stream.close
    assert_equal [[:ack, 0], [:ack, 1], [:ack, 2]], 3.times.map { @callback.delivered.pop(true) }
  end

  def test_callback_capacity_timeout_is_a_prequeue_failure
    stream = open_with_callback(callback_queue_capacity: 1, flush_timeout_ms: 100)
    stream.ingest_record_offset('{}')
    stream.flush(timeout_ms: 2000)
    assert_equal [:ack, 0], started
    assert_raises(Zerobus::TimeoutError) { stream.ingest_record_offset('{}') }
    @callback.release(2)
    assert_equal 1, stream.ingest_record_offset('{}'), 'timeout allocated an offset'
    stream.flush(timeout_ms: 2000)
    stream.close
    assert_equal [[:ack, 0], [:ack, 1]], 2.times.map { @callback.delivered.pop(true) }
  end

  def test_close_does_not_wait_for_a_queue_slot_or_drop_callbacks_when_budget_expires
    stream = open_with_callback(callback_max_wait_time_ms: 0)
    stream.ingest_record_offset('{}')
    stream.flush
    assert_equal [:ack, 0], started
    stream.ingest_record_offset('{}')
    stream.flush
    Timeout.timeout(2) { stream.close }
    assert stream.is_closed
    assert stream.instance_variable_get(:@callback_thread).alive?
    assert stream.instance_variable_get(:@callbacks).closed?
    @callback.release(2)
    assert stream.instance_variable_get(:@callback_thread).join(2)
    assert_equal [[:ack, 0], [:ack, 1]], 2.times.map { @callback.delivered.pop(true) }
  end

  def test_close_wakes_producers_waiting_for_callback_capacity
    stream = open_with_callback(callback_queue_capacity: 1, callback_max_wait_time_ms: 0)
    stream.ingest_record_offset('{}')
    stream.flush
    assert_equal [:ack, 0], started
    rejected = Queue.new
    @producer = Thread.new do
      stream.ingest_record_offset('{}')
    rescue Zerobus::ClosedStreamError => error
      rejected << error
    end
    eventually { @producer.status == 'sleep' }
    Timeout.timeout(2) { stream.close }
    assert @producer.join(2)
    assert_instance_of Zerobus::ClosedStreamError, rejected.pop(true)
    @callback.release
    assert stream.instance_variable_get(:@callback_thread).join(2)
    assert_equal [:ack, 0], @callback.delivered.pop(true)
    assert @callback.delivered.empty?
  end

  def test_terminal_failure_preserves_error_callbacks_with_a_full_backlog
    transport = FakeTransport.new(auto_ack: false)
    stream = open_with_callback(transport: transport, callback_queue_capacity: 3, callback_max_wait_time_ms: 0)
    stream.ingest_record_offset('{}')
    eventually { transport.sent.size == 2 }
    transport.ack(0)
    stream.flush
    assert_equal [:ack, 0], started
    stream.ingest_records_offset(['{}', '{}'])
    stream.ingest_record_offset('{}')
    eventually { transport.sent.size == 4 }
    transport.inject(Zerobus::RpcError.new(7, 'denied'))
    assert_raises(Zerobus::RpcError) { stream.flush }
    Timeout.timeout(2) { assert_raises(Zerobus::RpcError) { stream.close } }
    assert_equal [['{}', '{}'], ['{}']], stream.get_unacked_batches
    assert_equal 2, stream.instance_variable_get(:@callbacks).size
    @callback.release(3)
    assert stream.instance_variable_get(:@callback_thread).join(2)
    assert_equal [[:ack, 0], [:error, 1], [:error, 2]], 3.times.map { @callback.delivered.pop(true) }
    assert @callback.delivered.empty?
  end

  def test_ack_racing_failed_close_cannot_duplicate_callbacks_or_remove_failed_records
    transport = FakeTransport.new
    ack_seen, release = Queue.new, Queue.new
    @ack_release = release
    original = transport.method(:next_message)
    transport.define_singleton_method(:next_message) do |timeout:|
      message = original.call(timeout: timeout)
      if message && Zerobus::Protocol.fields(message).key?(2)
        ack_seen << true
        release.pop
      end
      message
    end
    stream = open_with_callback(transport: transport, callback_queue_capacity: 1,
      flush_timeout_ms: 20, callback_max_wait_time_ms: 0)
    stream.ingest_record_offset('{}')
    Timeout.timeout(2) { ack_seen.pop }
    Timeout.timeout(2) { assert_raises(Zerobus::TimeoutError) { stream.close } }
    assert_equal [:error, 0], started
    assert_equal ['{}'], stream.get_unacked_records
    release << true
    assert stream.instance_variable_get(:@worker).join(2)
    @callback.release
    assert stream.instance_variable_get(:@callback_thread).join(2)
    assert_equal [[:error, 0]], [@callback.delivered.pop(true)]
    assert @callback.delivered.empty?
    assert_equal ['{}'], stream.get_unacked_records
  end

  def test_persistent_resume_preserves_callback_reservations
    first = FakeTransport.new(persistent: true, auto_ack: false)
    resumed = FakeTransport.new(persistent: true, watermark: 0)
    transports = [first, resumed]
    @callback = SlowCallback.new
    sdk = sdk_for(->(**_) { transports.shift })
    @stream = sdk.persistent_stream_builder.table(props.table_name).headers_provider(headers).json
      .callback_queue_capacity(1).ack_callback(@callback).recovery_backoff_ms(1).build
    @callback.stream = @stream
    assert_equal 1, @stream.options.callback_queue_capacity
    assert_equal 0, @stream.ingest_records_offset(['{}', '{}'])
    eventually { first.sent.size == 2 }
    first.inject(Zerobus::RpcError.new(14, 'disconnected after commit'))
    assert_equal [:ack, 0], started
    @stream.flush
    result = Queue.new
    @producer = Thread.new { result << @stream.ingest_record_offset('{}') }
    eventually { @producer.status == 'sleep' }
    assert result.empty?, 'resume watermark released callback capacity prematurely'
    @callback.release
    assert @producer.join(2)
    assert_equal 1, result.pop(true)
    @stream.flush
    @callback.release
    @stream.close
    assert_equal [[:ack, 0], [:ack, 1]], 2.times.map { @callback.delivered.pop(true) }
  end

  def test_callback_cannot_deadlock_itself_by_waiting_for_its_own_capacity
    callback = Object.new
    rejected = Queue.new
    callback.define_singleton_method(:on_ack) do |_|
      begin
        @stream.ingest_record_offset('{}')
      rescue Zerobus::ZerobusException => error
        rejected << error
        raise
      end
    end
    @stream = open_stream(sdk_for(->(**_) { FakeTransport.new }), options: options(
      ack_callback: callback, callback_queue_capacity: 1, callback_max_wait_time_ms: nil))
    callback.instance_variable_set(:@stream, @stream)
    @stream.ingest_record_offset('{}')
    error = Timeout.timeout(2) { rejected.pop }
    assert_match 'callback cannot wait for ingestion capacity on its own stream', error.message
    Timeout.timeout(2) { @stream.close }
    assert_equal [error], @stream.callback_errors
  end

  def test_callback_can_close_its_own_stream
    callback = Object.new
    finished = Queue.new
    callback.define_singleton_method(:on_ack) { |_| @stream.close; finished << true }
    @stream = open_stream(sdk_for(->(**_) { FakeTransport.new }), options: options(
      ack_callback: callback, callback_queue_capacity: 1))
    callback.instance_variable_set(:@stream, @stream)
    @stream.ingest_record_offset('{}')
    assert Timeout.timeout(2) { finished.pop }
    assert @stream.is_closed
    assert_empty @stream.callback_errors
  end

  def test_callback_exception_diagnostics_are_bounded_without_losing_delivery
    invoked = Queue.new
    callback = Object.new
    callback.define_singleton_method(:on_ack) do |offset|
      invoked << offset
      raise "callback #{offset}"
    end
    @stream = open_stream(sdk_for(->(**_) { FakeTransport.new }), options: options(
      ack_callback: callback, callback_queue_capacity: 2))
    10.times { @stream.ingest_record_offset('{}') }
    @stream.close
    assert_equal (0...10).to_a, 10.times.map { invoked.pop(true) }
    assert_equal ['callback 8', 'callback 9'], @stream.callback_errors.map(&:message)
    assert_empty @stream.callback_errors
  end

  def test_callback_capacity_does_not_throttle_streams_without_callbacks
    @stream = open_stream(sdk_for(->(**_) { FakeTransport.new }), options: options(callback_queue_capacity: 1))
    10.times { @stream.ingest_record_offset('{}') }
    @stream.flush
    @stream.close
    assert_empty @stream.get_unacked_records
  end
end
