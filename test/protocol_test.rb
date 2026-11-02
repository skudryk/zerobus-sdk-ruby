# frozen_string_literal: true
require_relative 'test_helper'
class ProtocolTest < Minitest::Test
  include TestSupport
  def test_known_wire_encoding
    # Independent fixtures for field tags and nested single/batch envelopes.
    assert_equal '0800120178', P.ingest(0, ['x'], 1, batch: false, persistent: false).unpack1('H*').delete_prefix('1205')
    assert_equal '12050800120178', P.ingest(0, ['x'], 1, batch: false, persistent: false).unpack1('H*')
    assert_equal '1a0a08001a060a01780a0179', P.ingest(0, ['x', 'y'], 2, batch: true, persistent: false).unpack1('H*')
    assert_equal '1a050800120178', P.ingest(0, ['x'], 1, batch: false, persistent: true).unpack1('H*')
  end
  def test_create_and_resume
    create = P.fields(P.fields(P.open_request(props, persistent: false))[1].first)
    assert_equal props.table_name, create[1].first
    assert_equal 2, create[4].first
    resume = P.fields(P.fields(P.open_request(props, persistent: true, stream_id: 'abc'))[2].first)
    assert_equal 'abc', resume[1].first
    assert_equal 2, resume[3].first
    assert_equal [:resumed, -1], P.response(P.bytes(2, ''.b), persistent: true)
    assert_equal [:resumed, 123], P.response(P.bytes(2, P.integer(1, 123)), persistent: true)
  end
  def test_fragmented_and_coalesced_frames
    data = P.frame('abc') + P.frame('') + P.frame('def')
    decoder = P::Decoder.new
    result = []
    data.each_byte { |byte| decoder.feed(byte.chr) { |message| result << message } }
    decoder.finish!
    assert_equal ['abc', '', 'def'], result
  end
  def test_malformed_messages
    assert_raises(Zerobus::NonRetriableException) { P.fields("\x0a\x05x".b) }
    assert_raises(Zerobus::NonRetriableException) { P.fields("\x80".b) }
    assert_raises(Zerobus::NonRetriableException) { P.fields("\x00".b) }
    assert_raises(Zerobus::NonRetriableException) { P::Decoder.new.feed([1, 0].pack('CN')) {} }
    assert_raises(Zerobus::NonRetriableException) { P::Decoder.new(max_bytes: 10).feed([0, 11].pack('CN')) {} }
    decoder = P::Decoder.new
    decoder.feed("\x00") {}
    assert_raises(Zerobus::NonRetriableException) { decoder.finish! }
  end
end
