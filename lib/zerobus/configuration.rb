# frozen_string_literal: true
require 'json'
module Zerobus
  class ZerobusException < StandardError; end
  class NonRetriableException < ZerobusException; end
  class TimeoutError < ZerobusException; end
  class ClosedStreamError < NonRetriableException; end
  class RpcError < ZerobusException
    attr_reader :code
    def initialize(code, message)
      @code = code
      super("gRPC #{code}: #{message}")
    end
    def retryable? = [1, 4, 8, 10, 13, 14, 16].include?(code)
  end

  module RecordType
    UNSPECIFIED = 0
    PROTO = 1
    JSON = 2
  end

  class TableProperties
    attr_reader :table_name, :descriptor_proto, :record_type
    def initialize(table_name, descriptor_proto = nil)
      raise ArgumentError, 'table_name must be catalog.schema.table' unless table_name.is_a?(String) && table_name.split('.', -1).size == 3 && table_name.split('.', -1).all? { |s| !s.strip.empty? }
      raise TypeError, 'descriptor_proto must be serialized FileDescriptorProto bytes' if descriptor_proto && !descriptor_proto.is_a?(String)
      @table_name = table_name.dup.freeze
      @descriptor_proto = descriptor_proto&.b&.freeze
      @record_type = descriptor_proto ? RecordType::PROTO : RecordType::JSON
      freeze
    end
    def record_format = {1 => 'proto', 2 => 'json'}.fetch(record_type)
  end

  class AckCallback
    def on_ack(offset); end
    def on_error(offset, message); end
  end

  class StreamConfigurationOptions

    DEFAULTS = {
      max_inflight_records: 1_000_000, recovery: true, recovery_timeout_ms: 15_000,
      recovery_backoff_ms: 2_000, recovery_retries: 4, server_lack_of_ack_timeout_ms: 60_000,
      flush_timeout_ms: 300_000, record_type: RecordType::UNSPECIFIED,
      stream_paused_max_wait_time_ms: nil, callback_max_wait_time_ms: 5_000,
      ack_callback: nil, callback_queue_capacity: 1000,
      max_ingest_payload_bytes: 10 * 1024 * 1024 - 64 * 1024
    }.freeze

    DEFAULTS.each_key { |key| attr_reader key }

    def initialize(**options)
      unknown = options.keys - DEFAULTS.keys
      raise ArgumentError, "unknown options: #{unknown.join(', ')}" unless unknown.empty?

      DEFAULTS.merge(options).each do |key, value|
        if key.to_s.end_with?('_ms', '_bytes') || [:max_inflight_records, :recovery_retries, :callback_queue_capacity].include?(key)
          nullable = [:stream_paused_max_wait_time_ms, :callback_max_wait_time_ms].include?(key)
          minimum = [:max_inflight_records, :callback_queue_capacity].include?(key) ? 1 : 0
          raise ArgumentError, "#{key} must be an integer >= #{minimum}" unless (nullable && value.nil?) || (value.is_a?(Integer) && value >= minimum)
        end
        instance_variable_set("@#{key}", value)
      end

      raise ArgumentError, 'recovery must be boolean' unless [true, false].include?(@recovery)
      raise ArgumentError, 'invalid record_type' unless [0, 1, 2].include?(@record_type)
      freeze
    end
  end
end
