# frozen_string_literal: true
require 'typhoeus'
require 'base64'
module Zerobus
  class HeadersProvider
    def get_headers = raise NotImplementedError, 'implement get_headers'
    def invalidate; end
  end

  class FederatedToken
    attr_reader :idp_token_supplier, :databricks_client_id
    def initialize(idp_token_supplier:, databricks_client_id: nil)
      raise ArgumentError, 'idp_token_supplier must be callable' unless idp_token_supplier.respond_to?(:call)
      @idp_token_supplier = idp_token_supplier
      @databricks_client_id = databricks_client_id
      freeze
    end
  end

  class OAuthHeadersProvider < HeadersProvider
    def initialize(host:, unity_catalog_url:, table_name:, client_id: nil, client_secret: nil, auth: nil)
      @endpoint = URI(unity_catalog_url.sub(%r{/+$}, '') + '/oidc/v1/token')
      raise ArgumentError, 'OAuth requires an HTTPS workspace URL' unless @endpoint.scheme == 'https' && @endpoint.host && !@endpoint.userinfo && !@endpoint.query && !@endpoint.fragment
      @workspace = URI(host).host.split('.').first
      @table = table_name
      @client_id, @client_secret, @auth = client_id, client_secret, auth
      @mutex = Mutex.new
    end
    def invalidate
      @mutex.synchronize { @token = nil }
    end
    def get_headers
      token = @mutex.synchronize do
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        if !@token || now >= @refresh_at
          begin
            fetch_token(now)
          rescue ZerobusException => error
            current = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            raise if error.is_a?(NonRetriableException) || !@token || !@expires_at || current >= @expires_at
          end
        end
        @token
      end
      {'authorization' => "Bearer #{token}", 'x-databricks-zerobus-table-name' => @table}
    end

    private

    def fetch_token(now)
      catalog, schema, = @table.split('.')
      details = [['CATALOG', catalog, ['USE CATALOG']],
                 ['SCHEMA', "#{catalog}.#{schema}", ['USE SCHEMA']],
                 ['TABLE', @table, ['SELECT', 'MODIFY']]].map do |kind, path, privileges|
        item = {type: 'unity_catalog_privileges', object_type: kind, object_full_path: path, privileges: privileges}
        item[:operations] = ['zerobuswrite'] if kind == 'TABLE'
        item
      end
      form = {scope: 'all-apis', resource: "api://databricks/workspaces/#{@workspace}/zerobusDirectWriteApi",
              authorization_details: JSON.generate(details)}
      request_headers = {}
      subject = nil
      if @auth
        subject = @auth.idp_token_supplier.call
        raise ArgumentError, 'IdP token supplier must return a nonempty String' unless subject.is_a?(String) && !subject.strip.empty?
        form.merge!(grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange', subject_token: subject,
                    subject_token_type: 'urn:ietf:params:oauth:token-type:jwt')
        form[:client_id] = @auth.databricks_client_id if @auth.databricks_client_id
      else
        form[:grant_type] = 'client_credentials'
        request_headers['authorization'] = "Basic #{Base64.strict_encode64("#{@client_id}:#{@client_secret}")}"
      end
      request_headers['content-type'] = 'application/x-www-form-urlencoded'
      response = Typhoeus.post(@endpoint.to_s, body: URI.encode_www_form(form), headers: request_headers,
                               connecttimeout_ms: 15_000, timeout_ms: 15_000, followlocation: false,
                               ssl_verifypeer: true, ssl_verifyhost: 2)
      raise ZerobusException, "OAuth transport failed: #{response.return_code}" unless response.return_code == :ok
      code = response.code
      unless code.between?(200, 299)
        error = [408, 429].include?(code) || code >= 500 ? ZerobusException : NonRetriableException
        raise error, "OAuth token endpoint returned HTTP #{code}"
      end
      body = JSON.parse(response.body)
      token = body['access_token']
      raise NonRetriableException, 'OAuth response missing a usable access_token' unless token.is_a?(String) && !token.empty? && token.ascii_only? && !token.match?(/[\x00-\x20\x7f]/)
      ttl = begin
        Float(body.fetch('expires_in', 0))
      rescue ArgumentError, TypeError
        0
      end
      ttl = 0 unless ttl.finite? && ttl.positive?
      if subject
        begin
          payload = subject.split('.')[1]
          exp = JSON.parse(Base64.urlsafe_decode64(payload + '=' * ((4 - payload.length % 4) % 4)))['exp']
          if exp
            remaining = Float(exp) - Time.now.to_f
            raise NonRetriableException, 'IdP token already expired' unless remaining.positive?
            ttl = [ttl, remaining].min
          end
        rescue ArgumentError, TypeError, NoMethodError, JSON::ParserError
          # Opaque subject tokens cannot contribute an expiry cap.
        end
      end
      @token = token
      @expires_at = now + ttl
      @refresh_at = now + [ttl - 300, ttl * 0.8].max
    rescue JSON::ParserError
      raise NonRetriableException, 'invalid OAuth token response'
    end
  end
end
