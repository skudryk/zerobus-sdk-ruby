# frozen_string_literal: true
require_relative 'test_helper'
class ConfigurationTest < Minitest::Test
  include TestSupport
  def test_format_inference
    assert_equal 'json', props.record_format
    assert_equal 'proto', Zerobus::TableProperties.new('c.s.t', 'descriptor').record_format
    ['c.s', 'c.s.', '.s.t', 'c..t', ''].each do |name|
      assert_raises(ArgumentError) { Zerobus::TableProperties.new(name) }
    end
  end
  def test_option_validation
    [:flush_timeout_ms, :recovery_timeout_ms, :recovery_backoff_ms, :recovery_retries,
     :server_lack_of_ack_timeout_ms, :max_ingest_payload_bytes, :callback_max_wait_time_ms].each do |key|
      [-1, '1', true, 1.5].each { |value| assert_raises(ArgumentError) { options(**{key => value}) } }
    end
    assert_raises(ArgumentError) { options(max_inflight_records: 0) }
    assert_raises(ArgumentError) { options(recovery: 0) }
    assert_raises(ArgumentError) { options(unknown: 1) }
    assert_raises(ArgumentError) { options(record_type: 4) }
    assert_nil options(callback_max_wait_time_ms: nil).callback_max_wait_time_ms
    assert_raises(ArgumentError) do
      open_stream(sdk_for(->(**_) { flunk 'must validate before connecting' }), options: options(record_type: 1))
    end
  end
  def test_protobuf_bytes_are_opaque
    proto = Zerobus::Encoder.new(Zerobus::TableProperties.new('c.s.t', 'descriptor'))
    assert_equal "\x08\xff".b, proto.encode("\x08\xff".b)
    obj = Object.new
    def obj.to_proto = "\x08\x01".b
    assert_equal "\x08\x01".b, proto.encode(obj)
    assert_raises(TypeError) { proto.encode({}) }
  end
  def test_builder_validation_and_authentication
    sdk = sdk_for(->(**_) { flunk 'must validate before connecting' })
    assert_raises(ArgumentError) { sdk.persistent_stream_builder.build }
    assert_raises(ArgumentError) { sdk.persistent_stream_builder.table('c.s.t').json.build }
    assert_raises(ArgumentError) { sdk.persistent_stream_builder.table('c.s.t').json.resume('') }
    assert_raises(ArgumentError) { sdk.create_stream(table_properties: props) }
    auth = Zerobus::FederatedToken.new(idp_token_supplier: -> { 'token' })
    assert auth.frozen?
    assert_raises(ArgumentError) { sdk.create_stream(table_properties: props, auth: auth, client_id: 'id') }
    assert_raises(ArgumentError) { sdk.create_stream(table_properties: props, auth: Object.new) }
  end
end
