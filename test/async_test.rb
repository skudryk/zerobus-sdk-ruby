# frozen_string_literal: true
require_relative 'test_helper'
class AsyncTest < Minitest::Test
  include TestSupport
  def test_async_submission_and_durability
    sdk = Zerobus::Async::ZerobusSdk.new(host: 'http://localhost', unity_catalog_url: 'https://workspace.example',
                                        transport_factory: ->(**_) { FakeTransport.new })
    stream = sdk.create_stream(table_properties: props, headers_provider: headers, options: options).value
    begin
      offsets = 10.times.map { stream.ingest_record_offset('{}') }.map(&:value)
      assert_equal (0...10).to_a, offsets.sort
      stream.flush.value
      assert_equal 'stream-123', stream.stream_id
    ensure
      stream.close.value
    end
    assert stream.is_closed
    assert_raises(Zerobus::ClosedStreamError) { stream.ingest_record_offset('{}').value }
  ensure
    sdk&.shutdown
  end
  def test_delayed_acks_bound_workers_and_apply_backpressure_before_returning_future
    transport = FakeTransport.new(auto_ack: false)
    sdk = Zerobus::Async::ZerobusSdk.new(host: 'http://localhost', unity_catalog_url: 'https://workspace.example',
      worker_count: 2, queue_capacity: 3, transport_factory: ->(**_) { transport })
    stream = sdk.create_stream(table_properties: props, headers_provider: headers,
      options: options(max_inflight_records: 1, flush_timeout_ms: 5_000)).value(timeout: 2)
    assert_equal 0, stream.ingest_record_offset('{}').value(timeout: 2)
    eventually { transport.sent.size == 2 }
    baseline = Thread.list
    started = Queue.new
    original = stream.inner.method(:ingest_record_offset)
    stream.inner.define_singleton_method(:ingest_record_offset) do |payload|
      started << Thread.current
      original.call(payload)
    end

    futures = 2.times.map { stream.ingest_record_offset('{}') }
    workers = 2.times.map { Timeout.timeout(2) { started.pop } }
    assert_equal 2, workers.uniq.size
    3.times { futures << stream.ingest_record_offset('{}') }
    returned = Queue.new
    producer = Thread.new { returned << stream.ingest_record_offset('{}') }
    eventually { producer.status == 'sleep' }
    assert returned.empty?, 'submission returned while the work queue was full'
    assert_equal [producer], Thread.list - baseline, 'async calls allocated extra threads'
    refute futures.any?(&:complete?)

    transport.ack(0)
    assert producer.join(2), 'submission did not resume when queue capacity became available'
    futures << returned.pop(true)
    (1..6).each do |offset|
      eventually { transport.sent.size == offset + 2 }
      transport.ack(offset)
    end
    assert_equal (1..6).to_a, futures.map { |future| future.value(timeout: 2) }.sort
    stream.flush.value(timeout: 2)
    stream.close.value(timeout: 2)
    sdk.shutdown
    refute workers.any?(&:alive?)
  ensure
    # Release admission waits even if an assertion fails before ACKs resume.
    transport&.inject(Zerobus::RpcError.new(7, 'test cleanup'))
    stream&.inner&.close rescue nil
    sdk&.shutdown
    producer&.join(2)
  end

  def test_all_sdk_operations_share_the_same_executor
    calls = Queue.new
    inner_stream = Object.new
    inner_stream.define_singleton_method(:ingest_record_offset) { |_| calls << Thread.current; 42 }
    builder = Object.new
    builder.define_singleton_method(:table) { |_| self }
    builder.define_singleton_method(:build) { calls << Thread.current; inner_stream }
    builder.define_singleton_method(:resume) { |_| calls << Thread.current; inner_stream }
    inner_sdk = Object.new
    inner_sdk.define_singleton_method(:create_stream) { |**_| calls << Thread.current; inner_stream }
    inner_sdk.define_singleton_method(:recreate_stream) { |_| calls << Thread.current; inner_stream }
    inner_sdk.define_singleton_method(:retire_stream) { |**_| calls << Thread.current; :retired }
    inner_sdk.define_singleton_method(:persistent_stream_builder) { builder }
    sdk = nil
    ::Zerobus::ZerobusSdk.stub(:new, inner_sdk) do
      sdk = Zerobus::Async::ZerobusSdk.new(worker_count: 1, queue_capacity: 1)
    end
    stream = sdk.create_stream.value(timeout: 2)
    assert_equal 42, stream.ingest_record_offset('{}').value(timeout: 2)
    recreated = sdk.recreate_stream(stream).value(timeout: 2)
    assert_same inner_stream, recreated.inner
    assert_equal :retired, sdk.retire_stream(table_name: 'c.s.t', stream_id: 'id').value(timeout: 2)
    persistent = sdk.persistent_stream_builder.table('c.s.t')
    assert_equal 42, persistent.build.value(timeout: 2).ingest_record_offset('{}').value(timeout: 2)
    assert_equal 42, persistent.resume('saved-id').value(timeout: 2).ingest_record_offset('{}').value(timeout: 2)
    assert_equal 1, 8.times.map { calls.pop(true) }.uniq.size
    sdk.shutdown
    assert_raises(Zerobus::Async::ExecutorShutdownError) { sdk.create_stream }
  ensure
    sdk&.shutdown
  end

  def test_default_futures_share_a_bounded_pool
    first = Zerobus::Future.new { Thread.current }.value(timeout: 2)
    baseline = Thread.list
    workers = 20.times.map { Zerobus::Future.new { Thread.current } }.map { |future| future.value(timeout: 2) }
    assert_operator ([first] + workers).uniq.size, :<=, Zerobus::Async::Executor::DEFAULT_WORKER_COUNT
    assert_empty Thread.list - baseline
  end

  def test_future_errors_propagate
    assert_raises(ArgumentError) { Zerobus::Future.new { raise ArgumentError, 'bad input' }.value }
    release = Queue.new
    future = Zerobus::Future.new { release.pop }
    assert_raises(Zerobus::TimeoutError) { future.value(timeout: 0.001) }
    release << 42
    assert_equal 42, future.value
    assert future.complete?
  end
end
