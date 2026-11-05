# frozen_string_literal: true
require_relative 'test_helper'

class HandshakeTest < Minitest::Test
  include TestSupport

  class EndedTransport < FakeTransport
    attr_reader :reads, :closed

    def initialize(completion_error: nil)
      super()
      @reads = 0
      @completion_error = completion_error
    end

    def next_message(timeout:)
      @reads += 1
      nil
    end

    def completed?
      raise @completion_error if @completion_error
      true
    end

    def close
      @closed = true
      super
    end
  end

  def teardown
    @stream&.close rescue nil
  end

  def assert_premature_end
    error = assert_raises(Zerobus::ZerobusException) { yield }
    assert_instance_of Zerobus::ZerobusException, error # Not the handshake timeout subclass.
    assert_match 'before handshake completed', error.message
  end

  def test_ephemeral_end_before_handshake_stops_after_one_read
    transport = EndedTransport.new
    assert_premature_end do
      open_stream(sdk_for(->(**_) { transport }), options: options(recovery: false))
    end
    assert_equal 1, transport.reads
    assert transport.closed
  end

  def test_persistent_create_and_resume_detect_premature_end
    [:build, :resume].each do |operation|
      transport = EndedTransport.new
      builder = sdk_for(->(**_) { transport }).persistent_stream_builder
        .table(props.table_name).headers_provider(headers).json.recovery(false)
      assert_premature_end do
        operation == :build ? builder.build : builder.resume('saved-id')
      end
      assert_equal 1, transport.reads
      assert transport.closed
    end
  end

  def test_premature_end_enters_normal_recovery_without_spinning
    ended = EndedTransport.new
    healthy = FakeTransport.new
    peers = [ended, healthy]
    @stream = open_stream(sdk_for(->(**_) { peers.shift }))
    assert_equal 1, ended.reads
    assert ended.closed
    assert peers.empty?
    assert_equal 0, @stream.ingest_record_offset('{}')
    @stream.flush
  end

  def test_retries_are_bounded_when_every_rpc_ends_before_handshake
    peers = Array.new(3) { EndedTransport.new }
    attempts = 0
    sdk = sdk_for(->(**_) { transport = peers.fetch(attempts); attempts += 1; transport })
    assert_premature_end { open_stream(sdk, options: options(recovery_retries: 2)) }
    assert_equal 3, attempts
    assert_equal [1, 1, 1], peers.map(&:reads)
    assert peers.all?(&:closed)
  end

  def test_empty_read_from_live_transport_still_allows_handshake
    transport = FakeTransport.new
    original = transport.method(:next_message)
    reads = 0
    transport.define_singleton_method(:next_message) do |timeout:|
      reads += 1
      reads == 1 ? nil : original.call(timeout: timeout)
    end
    transport.define_singleton_method(:completed?) { false }
    @stream = open_stream(sdk_for(->(**_) { transport }), options: options(recovery: false))
    assert_equal 'stream-123', @stream.stream_id
    assert_equal 0, @stream.ingest_record_offset('{}')
    @stream.flush
  end

  def test_error_from_completion_check_keeps_original_rpc_status
    failure = Zerobus::RpcError.new(7, 'access denied')
    transport = EndedTransport.new(completion_error: failure)
    error = assert_raises(Zerobus::RpcError) do
      open_stream(sdk_for(->(**_) { transport }))
    end
    assert_same failure, error
    assert_equal 7, error.code
    assert_equal 1, transport.reads
    assert transport.closed
  end
end
