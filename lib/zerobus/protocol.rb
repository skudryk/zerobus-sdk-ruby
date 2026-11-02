# frozen_string_literal: true
module Zerobus
  # Minimal protobuf wire codec for the checked-in service schema. Record payloads
  # stay opaque; applications can use any protobuf implementation to encode them.
  module Protocol
    module_function

    def varint(value)
      value &= (1 << 64) - 1
      bytes = ''.b
      while value >= 128
        bytes << ((value & 127) | 128)
        value >>= 7
      end
      bytes << value
    end

    def integer(field, value) = varint(field << 3) + varint(value)

    def bytes(field, value) = varint((field << 3) | 2) + varint(value.bytesize) + value.b

    def read_varint(data, cursor)
      value = 0
      10.times do |index|
        byte = data.getbyte(cursor)
        raise NonRetriableException, 'truncated protobuf varint' unless byte
        cursor += 1
        raise NonRetriableException, 'protobuf varint overflow' if index == 9 && byte > 1
        value |= (byte & 127) << (7 * index)
        return [value, cursor] if byte < 128
      end
      raise NonRetriableException, 'invalid protobuf varint'
    end

    def fields(data)
      result = Hash.new { |hash, key| hash[key] = [] }
      cursor = 0

      while cursor < data.bytesize
        tag, cursor = read_varint(data, cursor)
        field, wire = tag >> 3, tag & 7
        raise NonRetriableException, 'invalid protobuf field' if field.zero?

        case wire
        when 0 then value, cursor = read_varint(data, cursor)
        when 1, 2, 5
          length = {1 => 8, 5 => 4}[wire]
          length, cursor = read_varint(data, cursor) if wire == 2
          raise NonRetriableException, 'truncated protobuf field' if cursor + length > data.bytesize

          value = data.byteslice(cursor, length)
          cursor += length
        else raise NonRetriableException, "unsupported protobuf wire type #{wire}"
        end

        result[field] << value
      end
      result
    end

    def create(properties)
      message = bytes(1, properties.table_name) + integer(4, properties.record_type)
      message << bytes(3, properties.descriptor_proto) if properties.descriptor_proto
      message
    end

    def open_request(properties, persistent:, stream_id: nil)
      return bytes(1, create(properties)) unless persistent

      if stream_id
        resume = bytes(1, stream_id) + integer(3, properties.record_type)
        resume << bytes(2, properties.descriptor_proto) if properties.descriptor_proto
        bytes(2, resume)
      else
        bytes(1, bytes(1, create(properties)))
      end
    end

    def ingest(offset, records, type, batch:, persistent:)
      record_field = {1 => 2, 2 => 3}.fetch(type)
      payload =
        if batch
          encoded = records.map { |record| bytes(1, record) }.join.b
          integer(1, offset) + bytes(record_field, encoded)
        else
          integer(1, offset) + bytes(record_field, records.fetch(0))
        end
      bytes((batch ? 3 : 2) + (persistent ? 1 : 0), payload)
    end

    def response(data, persistent:)
      top = fields(data)

      if top.key?(1)
        id = fields(top[1].first)[1].first
        raise NonRetriableException, 'missing stream id' unless id && !id.empty?

        [:created, id.force_encoding(Encoding::UTF_8)]
      elsif persistent && top.key?(2)
        watermark = fields(top[2].first)[1].first
        watermark = -1 if watermark.nil? || watermark == (1 << 64) - 1
        [:resumed, watermark]
      elsif top.key?(persistent ? 3 : 2)
        ack = fields(top[persistent ? 3 : 2].first)[1].first
        raise NonRetriableException, 'missing acknowledgment offset' unless ack

        [:ack, ack]
      elsif top.key?(persistent ? 4 : 3)
        duration = fields(top[persistent ? 4 : 3].first)[1].first
        parts = duration ? fields(duration) : {}
        [:rotate, (parts.fetch(1, [0]).first + parts.fetch(2, [0]).first / 1e9)]
      else
        [:unknown, nil]
      end
    end

    def frame(message) = [0, message.bytesize].pack('CN') + message.b

    class Decoder

      def initialize(max_bytes: 16 * 1024 * 1024)
        @buffer = ''.b
        @max_bytes = max_bytes
      end

      def feed(chunk)
        @buffer << chunk.b

        while @buffer.bytesize >= 5
          compression, length = @buffer.unpack('CN')
          raise NonRetriableException, 'compressed gRPC responses are unsupported' unless compression.zero?

          raise NonRetriableException, 'gRPC response too large' if length > @max_bytes

          break if @buffer.bytesize < length + 5
          message = @buffer.byteslice(5, length)
          @buffer = @buffer.byteslice(length + 5..)

          yield message
        end
      end

      def finish!
        raise NonRetriableException, 'truncated gRPC frame' unless @buffer.empty?
      end
    end
  end
end
