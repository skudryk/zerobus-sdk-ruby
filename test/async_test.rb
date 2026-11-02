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
