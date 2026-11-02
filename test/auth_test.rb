# frozen_string_literal: true
require_relative 'test_helper'
class AuthTest < Minitest::Test
  include TestSupport
  def with_post_stub(value, &block)
    # Typhoeus defines its own .stub, so call Minitest's helper explicitly.
    Object.instance_method(:stub).bind_call(Typhoeus, :post, value, &block)
  end
  def provider(auth: nil)
    Zerobus::OAuthHeadersProvider.new(host: 'https://123.zerobus.example', unity_catalog_url: 'https://workspace.example',
      table_name: props.table_name, client_id: 'id', client_secret: 'secret', auth: auth)
  end
  def response(code: 200, token: 'token', expires_in: 3600, return_code: :ok)
    Typhoeus::Response.new(code: code, return_code: return_code,
                           body: JSON.generate(access_token: token, expires_in: expires_in))
  end
  def test_oauth_scope_and_cache
    requests = []
    stub = lambda do |url, options|
      requests << [url, options]
      response
    end
    p = provider
    with_post_stub(stub) do
      assert_equal 'Bearer token', p.get_headers['authorization']
      assert_equal 'Bearer token', p.get_headers['authorization']
      assert_equal 1, requests.size
      p.invalidate
      p.get_headers
      assert_equal 2, requests.size
    end
    url, opts = requests.first
    assert_equal 'https://workspace.example/oidc/v1/token', url
    assert_equal "Basic #{Base64.strict_encode64('id:secret')}", opts[:headers]['authorization']
    form = URI.decode_www_form(opts[:body]).to_h
    assert_equal 'client_credentials', form['grant_type']
    assert_equal 'all-apis', form['scope']
    assert_equal 'api://databricks/workspaces/123/zerobusDirectWriteApi', form['resource']
    details = JSON.parse(form['authorization_details'])
    assert_equal [['USE CATALOG'], ['USE SCHEMA'], ['SELECT', 'MODIFY']], details.map { |item| item['privileges'] }
    assert_equal ['catalog', 'catalog.schema', 'catalog.schema.events'], details.map { |item| item['object_full_path'] }
    assert_equal ['zerobuswrite'], details.last['operations']
    assert_equal false, opts[:followlocation]
    assert_equal true, opts[:ssl_verifypeer]
  end
  def test_federation_with_and_without_service_principal
    [nil, 'service-principal'].each do |client_id|
      calls = 0
      token = Zerobus::FederatedToken.new(idp_token_supplier: -> { calls += 1; 'external-token' }, databricks_client_id: client_id)
      with_post_stub(lambda { |_url, opts|
        form = URI.decode_www_form(opts[:body]).to_h
        assert_equal 'urn:ietf:params:oauth:grant-type:token-exchange', form['grant_type']
        assert_equal 'external-token', form['subject_token']
        assert_equal 'urn:ietf:params:oauth:token-type:jwt', form['subject_token_type']
        client_id.nil? ? assert_nil(form['client_id']) : assert_equal(client_id, form['client_id'])
        refute opts[:headers].key?('authorization')
        response
      }) do
        p = provider(auth: token)
        2.times { p.get_headers }
        assert_equal 1, calls
      end
    end
  end
  def test_failures_and_bad_tokens
    [301, 400, 401, 403].each do |code|
      with_post_stub(response(code: code)) { assert_raises(Zerobus::NonRetriableException) { provider.get_headers } }
    end
    [408, 429, 503].each do |code|
      with_post_stub(response(code: code)) { assert_raises(Zerobus::ZerobusException) { provider.get_headers } }
    end
    ['', "bad\ntoken", '   '].each do |token|
      with_post_stub(response(token: token)) { assert_raises(Zerobus::NonRetriableException) { provider.get_headers } }
    end
    with_post_stub(response(return_code: :couldnt_connect)) { assert_raises(Zerobus::ZerobusException) { provider.get_headers } }
  end
  def test_no_expiry_is_not_cached
    calls = 0
    with_post_stub(->(*_) { calls += 1; response(expires_in: nil) }) do
      p = provider
      2.times { p.get_headers }
      assert_equal 2, calls
    end
  end
  def test_transient_refresh_failure_uses_still_valid_token
    p = provider
    with_post_stub(response) { p.get_headers }
    p.instance_variable_set(:@refresh_at, 0)
    with_post_stub(response(code: 503)) do
      assert_equal 'Bearer token', p.get_headers['authorization']
    end
    with_post_stub(response(code: 401)) do
      assert_raises(Zerobus::NonRetriableException) { p.get_headers }
    end
    p.instance_variable_set(:@expires_at, 0)
    with_post_stub(response(code: 503)) do
      assert_raises(Zerobus::ZerobusException) { p.get_headers }
    end
  end
  def test_expired_subject_token_is_rejected
    payload = Base64.urlsafe_encode64(JSON.generate(exp: Time.now.to_i - 1), padding: false)
    auth = Zerobus::FederatedToken.new(idp_token_supplier: -> { "header.#{payload}.sig" })
    with_post_stub(response) { assert_raises(Zerobus::NonRetriableException) { provider(auth: auth).get_headers } }
  end
end
