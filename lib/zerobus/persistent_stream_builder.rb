# frozen_string_literal: true
module Zerobus
  class PersistentStreamBuilder
    def initialize(sdk)
      @sdk, @settings, @authentication = sdk, {}, {}
    end
    def table(name)
      @table = name
      self
    end
    def oauth(client_id, client_secret)
      @authentication = {client_id: client_id, client_secret: client_secret}
      self
    end
    def headers_provider(provider)
      @authentication = {headers_provider: provider}
      self
    end
    def json
      @format, @descriptor = :json, nil
      self
    end
    def compiled_proto(descriptor)
      @format, @descriptor = :proto, descriptor
      self
    end
    (StreamConfigurationOptions::DEFAULTS.keys - [:record_type, :max_inflight_records, :stream_paused_max_wait_time_ms]).each do |key|
      define_method(key) do |value|
        StreamConfigurationOptions.new(**@settings.merge(key => value))
        @settings[key] = value
        self
      end
    end
    def max_inflight_requests(value)
      StreamConfigurationOptions.new(max_inflight_records: value)
      @settings[:max_inflight_records] = value
      self
    end
    def build = open_stream(nil)
    def resume(stream_id)
      raise ArgumentError, 'stream_id must be a nonempty String' unless stream_id.is_a?(String) && !stream_id.empty?
      open_stream(stream_id)
    end

    private

    def open_stream(stream_id)
      raise ArgumentError, 'select json or compiled_proto before building' unless @format
      raise ArgumentError, 'compiled_proto requires descriptor bytes' if @format == :proto && @descriptor.nil?
      @sdk.open_persistent_stream(table_properties: TableProperties.new(@table, @descriptor),
                                  options: StreamConfigurationOptions.new(**@settings), stream_id: stream_id, **@authentication)
    end
  end
end
