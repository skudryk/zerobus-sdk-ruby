# frozen_string_literal: true
module Zerobus
  class Encoder
    def initialize(properties)
      @properties = properties
    end
    def encode(payload)
      case @properties.record_type
      when RecordType::JSON
        encoded = payload.is_a?(Hash) ? JSON.generate(payload) : payload
        raise TypeError, 'JSON payload must be a String or Hash' unless encoded.is_a?(String)
        text = encoded.dup.force_encoding(Encoding::UTF_8)
        raise ArgumentError, 'JSON payload must be valid UTF-8' unless text.valid_encoding?
        parsed = JSON.parse(text)
        raise ArgumentError, 'JSON record must be an object' unless parsed.is_a?(Hash)
        text.b.freeze
      when RecordType::PROTO
        encoded = payload.is_a?(String) ? payload : (payload.to_proto if payload.respond_to?(:to_proto))
        raise TypeError, 'protobuf payload must be bytes or respond to to_proto' unless encoded.is_a?(String)
        encoded.b.freeze
      end
    end
  end
end
