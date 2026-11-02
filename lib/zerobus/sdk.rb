# frozen_string_literal: true
module Zerobus
  class ZerobusSdk
    attr_reader :host, :unity_catalog_url
    def initialize(host:, unity_catalog_url:, application_name: nil, transport_factory: nil)
      uri = URI(host)
      unless ['http', 'https'].include?(uri.scheme) && uri.host && !uri.userinfo && !uri.query && !uri.fragment && ['', '/'].include?(uri.path)
        raise ArgumentError, 'host must be an HTTP(S) origin URL'
      end
      @host, @unity_catalog_url = host, unity_catalog_url
      @user_agent = "zerobus-sdk-ruby-sdk/#{VERSION}"
      @user_agent += " #{application_name}" if application_name
      @transport_factory = transport_factory || Transport.method(:new)
      @providers, @providers_mutex = {}, Mutex.new
    end
    def create_stream(client_id: nil, client_secret: nil, table_properties:, options: StreamConfigurationOptions.new,
                      headers_provider: nil, auth: nil)
      validate_format(table_properties, options)
      provider = provider_for(table_properties, client_id, client_secret, headers_provider, auth)
      ZerobusStream.new(sdk: self, table_properties: table_properties, options: options, headers_provider: provider)
    end
    def persistent_stream_builder = PersistentStreamBuilder.new(self)
    def recreate_stream(old_stream)
      raise ArgumentError, 'old stream must be closed' unless old_stream.is_closed
      raise ArgumentError, 'persistent streams should be resumed using their stream id' if old_stream.is_a?(PersistentStream)
      stream = create_stream(table_properties: old_stream.table_properties, options: old_stream.options,
                             headers_provider: old_stream.headers_provider)
      old_stream.get_unacked_batches.each { |batch| stream.ingest_records_offset(batch) }
      stream
    end
    def open_persistent_stream(table_properties:, options:, client_id: nil, client_secret: nil, headers_provider: nil, stream_id: nil)
      validate_format(table_properties, options)
      raise ArgumentError, 'stream_id must be a nonempty String' if stream_id && (!stream_id.is_a?(String) || stream_id.empty?)
      provider = provider_for(table_properties, client_id, client_secret, headers_provider, nil)
      PersistentStream.new(sdk: self, table_properties: table_properties, options: options,
                           headers_provider: provider, persistent: true, stream_id: stream_id)
    end
    def retire_stream(table_name:, stream_id:, client_id: nil, client_secret: nil, headers_provider: nil)
      properties = TableProperties.new(table_name)
      raise ArgumentError, 'stream_id must be a nonempty String' unless stream_id.is_a?(String) && !stream_id.empty?
      provider = provider_for(properties, client_id, client_secret, headers_provider, nil)
      transport = open_transport(provider, properties, 'RetireStream', StreamConfigurationOptions.new)
      transport.send_message(Protocol.bytes(1, table_name) + Protocol.bytes(2, stream_id))
      transport.finish_request
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
      until transport.completed?
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise TimeoutError, 'retire_stream timed out' if remaining <= 0
        transport.next_message(timeout: [remaining, 0.1].min)
      end
      nil
    ensure
      transport&.close
    end
    # Transport factory is injectable to allow testing without Databricks credentials.
    def open_transport(provider, properties, method, options)
      headers = provider.respond_to?(:get_headers) ? provider.get_headers : provider.call
      headers = headers.to_h.transform_keys { |key| key.to_s.downcase }
      headers['x-databricks-zerobus-table-name'] ||= properties.table_name
      headers['user-agent'] = @user_agent
      headers.each do |key, value|
        raise ArgumentError, 'invalid authentication metadata' unless key.match?(/\A[a-z0-9_.-]+\z/) && value.is_a?(String) && value.ascii_only? && !value.match?(/[\x00-\x1f\x7f]/)
      end
      @transport_factory.call(host: host, headers: headers, method: method, timeout: options.recovery_timeout_ms / 1000.0)
    end

    private

    def validate_format(properties, options)
      raise TypeError, 'table_properties must be TableProperties' unless properties.is_a?(TableProperties)
      raise TypeError, 'options must be StreamConfigurationOptions' unless options.is_a?(StreamConfigurationOptions)
      unless options.record_type == RecordType::UNSPECIFIED || options.record_type == properties.record_type
        raise ArgumentError, 'record_type does not match TableProperties'
      end
    end
    def provider_for(properties, client_id, client_secret, headers_provider, auth)
      raise ArgumentError, 'client credentials cannot be combined with auth' if auth && (client_id || client_secret)
      raise ArgumentError, 'auth must be a FederatedToken' if auth && !auth.is_a?(FederatedToken)
      return headers_provider if headers_provider && !auth
      if !auth && (!client_id.is_a?(String) || client_id.empty? || !client_secret.is_a?(String) || client_secret.empty?)
        raise ArgumentError, 'client_id and client_secret are required without auth or headers_provider'
      end
      key = [properties.table_name, client_id, client_secret, auth]
      @providers_mutex.synchronize do
        @providers[key] ||= OAuthHeadersProvider.new(host: host, unity_catalog_url: unity_catalog_url,
          table_name: properties.table_name, client_id: client_id, client_secret: client_secret, auth: auth)
      end
    end
  end
end
