# frozen_string_literal: true
require_relative 'test_helper'
class StreamTest < Minitest::Test
  include TestSupport
  class Callback < Zerobus::AckCallback
    attr_reader :events
    def initialize = @events = Queue.new
    def on_ack(offset) = @events << [:ack, offset]
    def on_error(offset, message) = @events << [:error, offset]
  end
  def teardown
    @streams&.each { |stream| stream.close rescue nil }
  end
  def track(stream)
    (@streams ||= []) << stream
    stream
  end
  def test_queue_batch_flush_and_callback
    transport = FakeTransport.new(auto_ack: false)
    callback = Callback.new
    stream = track(open_stream(sdk_for(->(**_) { transport }), options: options(ack_callback: callback)))
    assert_equal 0, stream.ingest_record_offset({'n' => 1})
    assert_equal 1, stream.ingest_records_offset(['{"n":2}', '{"n":3}'])
    assert_nil stream.ingest_records_offset([])
    eventually { transport.sent.size == 3 }
    transport.ack(1)
    stream.wait_for_offset(1)
    stream.close
    assert_equal [[:ack, 0], [:ack, 1]], 2.times.map { callback.events.pop }
    assert_equal [], stream.get_unacked_records
    assert_equal 'stream-123', stream.stream_id
    assert_raises(Zerobus::ClosedStreamError) { stream.ingest_record_offset('{}') }
  end
  def test_prequeue_validation_has_no_callback_or_offset
    callback = Callback.new
    stream = track(open_stream(sdk_for(->(**_) { FakeTransport.new }), options: options(ack_callback: callback, max_ingest_payload_bytes: 10)))
    assert_raises(JSON::ParserError) { stream.ingest_record_offset('bad') }
    assert_raises(ArgumentError) { stream.ingest_record_offset('[]') }
    assert_raises(ArgumentError) { stream.ingest_record_offset('{"long":"payload"}') }
    assert_raises(TypeError) { stream.ingest_records_offset('{}') }
    assert_equal 0, stream.ingest_record_offset('{}')
    stream.close
    assert_equal [:ack, 0], callback.events.pop
    assert callback.events.empty?
  end
  def test_recovery_replays_pending_with_fresh_wire_offsets
    first = FakeTransport.new(auto_ack: false)
    second = FakeTransport.new
    transports = [first, second]
    stream = track(open_stream(sdk_for(->(**_) { transports.shift })))
    stream.ingest_record_offset('{"n":0}')
    eventually { first.sent.size == 2 }
    first.ack(0)
    stream.wait_for_offset(0)
    stream.ingest_record_offset('{"n":1}')
    eventually { first.sent.size == 3 }
    first.inject(Zerobus::RpcError.new(14, 'unavailable'))
    stream.flush
    assert_equal 2, second.sent.size
    second.sent.pop
    replay = P.fields(P.fields(second.sent.pop)[2].first)
    assert_equal 0, replay[1].first
    assert_equal '{"n":1}', replay[3].first
    assert_equal 2, stream.ingest_record_offset('{}')
  end
  def test_non_retryable_retains_data_and_reports_one_error_per_submission
    transport = FakeTransport.new(auto_ack: false)
    callback = Callback.new
    count = 0
    stream = track(open_stream(sdk_for(->(**_) { count += 1; transport }), options: options(ack_callback: callback)))
    stream.ingest_records_offset(['{}', '{}'])
    eventually { transport.sent.size == 2 }
    transport.inject(Zerobus::RpcError.new(7, 'denied'))
    assert_raises(Zerobus::RpcError) { stream.flush }
    assert_raises(Zerobus::RpcError) { stream.close }
    assert_equal [['{}', '{}']], stream.get_unacked_batches
    assert_equal [:error, 0], callback.events.pop
    assert callback.events.empty?
    assert_equal 1, count
  end
  def test_inflight_backpressure_and_concurrent_admission
    transport = FakeTransport.new(auto_ack: false)
    stream = track(open_stream(sdk_for(->(**_) { transport }), options: options(max_inflight_records: 1)))
    stream.ingest_record_offset('{}')
    submitted = Queue.new
    producer = Thread.new { submitted << stream.ingest_record_offset('{}') }
    eventually { transport.sent.size == 2 }
    assert submitted.empty?
    transport.ack(0)
    assert producer.join(2)
    assert_equal 1, submitted.pop
    eventually { transport.sent.size == 3 }
    transport.ack(1)
    stream.flush
  end
  def test_timeout_retains_records_and_close_is_bounded
    transport = FakeTransport.new(auto_ack: false)
    stream = track(open_stream(sdk_for(->(**_) { transport }), options: options(flush_timeout_ms: 20)))
    stream.ingest_record_offset('{}')
    assert_raises(Zerobus::TimeoutError) { stream.flush }
    assert_raises(Zerobus::TimeoutError) { stream.close }
    assert_equal ['{}'], stream.get_unacked_records
  end
  def test_ack_beyond_sent_is_terminal
    transport = FakeTransport.new(auto_ack: false)
    stream = track(open_stream(sdk_for(->(**_) { transport })))
    stream.ingest_record_offset('{}')
    eventually { transport.sent.size == 2 }
    transport.ack(50)
    assert_raises(Zerobus::NonRetriableException) { stream.flush }
  end
  def test_persistent_resume_and_recovery_use_durable_watermark
    first = FakeTransport.new(persistent: true, watermark: 7, auto_ack: false)
    second = FakeTransport.new(persistent: true, watermark: 8)
    transports = [first, second]
    sdk = sdk_for(->(**_) { transports.shift })
    stream = track(sdk.persistent_stream_builder.table(props.table_name).headers_provider(headers).json
                      .recovery_backoff_ms(1).flush_timeout_ms(2_000).resume('saved-id'))
    assert_equal 7, stream.resume_watermark
    assert_equal 8, stream.ingest_record_offset('{}')
    assert_equal 9, stream.ingest_record_offset('{}')
    eventually { first.sent.size == 3 }
    first.inject(Zerobus::RpcError.new(14, 'disconnected after commit'))
    stream.flush
    assert_equal 2, second.sent.size # resume + only offset 9 (8 was committed)
    second.sent.pop
    payload = P.fields(P.fields(second.sent.pop)[3].first)
    assert_equal 9, payload[1].first
    assert_equal 'saved-id', stream.stream_id
  end
  def test_rotation_waits_for_ack_then_replays_only_pending
    first = FakeTransport.new(auto_ack: false)
    second = FakeTransport.new
    transports = [first, second]
    stream = track(open_stream(sdk_for(->(**_) { transports.shift })))
    stream.ingest_record_offset('{}')
    eventually { first.sent.size == 2 }
    first.inject(P.bytes(3, P.bytes(1, P.integer(1, 1))))
    first.ack(0)
    stream.wait_for_offset(0)
    eventually { second.sent.size == 1 }
    stream.ingest_record_offset('{}')
    stream.flush
    assert_equal 2, second.sent.size
  end
  def test_concurrent_producers_get_unique_offsets
    stream = track(open_stream(sdk_for(->(**_) { FakeTransport.new })))
    offsets = 5.times.map { Thread.new { 20.times.map { stream.ingest_record_offset('{}') } } }.flat_map(&:value)
    stream.flush
    assert_equal (0...100).to_a, offsets.sort
  end
  def test_callback_failures_do_not_fail_ingestion
    callback = Object.new
    def callback.on_ack(_) = raise 'callback failure'
    stream = track(open_stream(sdk_for(->(**_) { FakeTransport.new }), options: options(ack_callback: callback)))
    stream.ingest_record_offset('{}')
    stream.close
    assert_equal 'callback failure', stream.callback_errors.first.message
  end
end
