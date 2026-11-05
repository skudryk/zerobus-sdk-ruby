# frozen_string_literal: true
module Zerobus
  class RecordAcknowledgment

    attr_reader :offset

    def initialize(stream, offset)
      @stream, @offset = stream, offset
    end

    def wait_for_ack = @stream.wait_for_offset(offset)
    alias wait wait_for_ack
  end

  class ZerobusStream
    Entry = Struct.new(:offset, :records, :batch, :wire, :sent_at, keyword_init: true)
    attr_reader :stream_id, :resume_watermark, :table_properties, :options, :headers_provider

    def initialize(sdk:, table_properties:, options:, headers_provider:, persistent: false, stream_id: nil)
      @sdk, @table_properties, @options, @headers_provider = sdk, table_properties, options, headers_provider
      @persistent, @stream_id = persistent, stream_id
      @encoder = build_encoder(table_properties)
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @pending = {}
      @unsent = []
      @inflight_used = 0
      @next_offset = 0
      @acked_offset = -1
      @state = :opening
      @callbacks = SizedQueue.new(options.callback_queue_capacity)
      @pending_callbacks = 0
      @callback_errors = []
      @callback_thread = Thread.new { run_callbacks } if options.ack_callback
      @worker = Thread.new { supervise }
      @worker.report_on_exception = false
      @mutex.synchronize do
        deadline = monotonic + options.recovery_timeout_ms / 1000.0
        until @ready || @error
          remaining = deadline - monotonic
          raise TimeoutError, 'stream creation timed out' if remaining <= 0
          @condition.wait(@mutex, remaining)
        end
        raise @error if @error
      end
    rescue StandardError
      @mutex.synchronize { @stop = true; @condition.broadcast } if @mutex
      @transport&.close
      @worker&.join(1)
      @callbacks&.close
      raise
    end

    def ingest_record_offset(payload) = enqueue([@encoder.encode(payload)], batch: false)

    def ingest_records_offset(payloads)
      raise TypeError, 'batch must be an enumerable of records' if payloads.is_a?(String) || payloads.is_a?(Hash) || !payloads.respond_to?(:each)

      records = payloads.map { |payload| @encoder.encode(payload) }
      return nil if records.empty?

      enqueue(records, batch: true)
    end

    def ingest_record(payload) = RecordAcknowledgment.new(self, ingest_record_offset(payload))

    # Ruby nowait methods still queue synchronously so validation failures remain
    # observable and flush always includes submissions made before it.
    def ingest_record_nowait(payload)
      ingest_record_offset(payload)
      nil
    end

    def ingest_records_nowait(payloads)
      ingest_records_offset(payloads)
      nil
    end

    def wait_for_offset(offset, timeout_ms: options.flush_timeout_ms)
      @mutex.synchronize do
        raise ArgumentError, 'offset was not submitted on this stream' unless offset.is_a?(Integer) && offset >= 0 && offset < @next_offset
        wait_locked(offset, monotonic + timeout_ms / 1000.0)
      end
      nil
    end

    def flush(timeout_ms: options.flush_timeout_ms)
      @mutex.synchronize { wait_locked(@next_offset - 1, monotonic + timeout_ms / 1000.0) }
      nil
    end

    def close
      error = nil
      @mutex.synchronize do
        @closing = true
        @condition.broadcast
        begin
          wait_locked(@next_offset - 1, monotonic + options.flush_timeout_ms / 1000.0)
          raise @error if @error
        rescue ZerobusException => exception
          error = exception
          terminal_locked(exception) unless @error
        ensure
          @stop = true
          @state = :closed
          @condition.broadcast
        end
      end

      @transport&.close
      @worker.join(1) unless Thread.current == @worker
      @callbacks.close
      unless !@callback_thread || Thread.current == @callback_thread
        budget = options.callback_max_wait_time_ms
        budget.nil? ? @callback_thread.join : @callback_thread.join(budget / 1000.0)
      end
      raise error if error
      nil
    end

    def is_closed = @mutex.synchronize { @state == :closed || @state == :failed }

    def get_state = @mutex.synchronize { @state }

    def get_unacked_batches
      @mutex.synchronize do
        raise ZerobusException, 'close the stream before retrieving unacknowledged records' unless [:closed, :failed].include?(@state)
        @pending.values.map { |entry| entry.records.map(&:dup) }
      end
    end

    def get_unacked_records = get_unacked_batches.flatten(1)

    def callback_errors
      @mutex.synchronize { @callback_errors.shift(@callback_errors.length) }
    end

    private

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def enqueue(records, batch:)
      total = records.sum(&:bytesize)
      raise ArgumentError, 'ingest payload exceeds max_ingest_payload_bytes' if total > options.max_ingest_payload_bytes

      count = @persistent ? 1 : records.size
      raise ArgumentError, 'submission exceeds inflight capacity' if count > options.max_inflight_records

      @mutex.synchronize do
        deadline = monotonic + options.flush_timeout_ms / 1000.0

        loop do
          raise @error if @error
          raise ClosedStreamError, 'stream is closed' if @closing || @stop
          callback_capacity = !options.ack_callback || @pending_callbacks < options.callback_queue_capacity
          break if @inflight_used + count <= options.max_inflight_records && callback_capacity

          if Thread.current == @callback_thread
            raise ZerobusException, 'callback cannot wait for ingestion capacity on its own stream'
          end

          remaining = deadline - monotonic
          raise TimeoutError, 'waiting for ingestion or callback capacity timed out' if remaining <= 0

          @condition.wait(@mutex, remaining)
        end
        offset = @next_offset

        raise NonRetriableException, 'offset overflow' if offset >= (1 << 63) - 1

        entry = Entry.new(offset: offset, records: records.freeze, batch: batch)
        @pending[offset] = entry
        @unsent << entry
        @inflight_used += count
        # Reserve delivery capacity before committing the submission. ACKs do
        # not release it; the callback worker does so after invocation finishes.
        @pending_callbacks += 1 if options.ack_callback
        @next_offset += 1
        @condition.broadcast
        offset
      end
    end

    def wait_locked(offset, deadline)
      while @acked_offset < offset
        raise @error if @error
        raise ClosedStreamError, 'stream closed before acknowledgment' if @stop

        remaining = deadline - monotonic
        raise TimeoutError, "waiting for offset #{offset} timed out" if remaining <= 0

        @condition.wait(@mutex, remaining)
      end
      raise @error if @error

    end

    def supervise
      retries = 0

      loop do
        break if stopped?
        begin
          open_connection
          active_loop
          break

        rescue StandardError => error
          break if stopped?
          retryable = ![NonRetriableException, ArgumentError, TypeError, NoMethodError].any? { |type| error.is_a?(type) } &&
                      (!error.is_a?(RpcError) || error.retryable?)
          if retryable && options.recovery && retries < options.recovery_retries
            retries += 1
            @headers_provider.invalidate if error.is_a?(RpcError) && error.code == 16 && @headers_provider.respond_to?(:invalidate)
            @mutex.synchronize do
              @state = :recovering
              @condition.wait(@mutex, options.recovery_backoff_ms / 1000.0) unless @stop
            end
          else
            @mutex.synchronize { terminal_locked(error.is_a?(ZerobusException) ? error : ZerobusException.new(error.message)) }
            break
          end

        ensure
          @transport&.close
        end
      end
    ensure
      @mutex.synchronize { @condition.broadcast }
      @callbacks.close
    end

    def build_encoder(properties) = Encoder.new(properties)

    def transport_method = @persistent ? 'PersistentStream' : 'EphemeralStream'

    def initial_message = Protocol.open_request(table_properties, persistent: @persistent, stream_id: @stream_id)

    def decode_response(message) = Protocol.response(message, persistent: @persistent)

    def handshake_kinds = [:created, :resumed]

    def ingest_message(entry, wire)
      Protocol.ingest(wire, entry.records, table_properties.record_type, batch: entry.batch, persistent: @persistent)
    end

    def apply_handshake_locked(kind, value)
      if kind == :created
        raise NonRetriableException, 'unexpected create response while resuming' if @persistent && @stream_id

        @stream_id = value
      else
        raise NonRetriableException, 'unexpected resume response' unless @persistent && @stream_id

        if value < @acked_offset
          raise NonRetriableException, 'resume watermark regressed below acknowledged data'
        end
        if @ready && value >= @next_offset
          raise NonRetriableException, 'resume watermark exceeds submitted offsets'
        end

        @resume_watermark = value
        @next_offset = value + 1 unless @ready
        acknowledge_locked(value)
      end
    end

    def stopped? = @mutex.synchronize { @stop }

    def open_connection
      @transport = @sdk.open_transport(@headers_provider, @table_properties,
                                       transport_method, options)
      @transport.send_message(initial_message)
      deadline = monotonic + options.recovery_timeout_ms / 1000.0
      loop do
        raise ClosedStreamError, 'stream closed' if stopped?

        remaining = deadline - monotonic
        raise TimeoutError, 'stream handshake timed out' if remaining <= 0

        message = @transport.next_message(timeout: [remaining, 0.1].min)
        next unless message

        kind, value = decode_response(message)
        unless handshake_kinds.include?(kind)
          raise NonRetriableException, 'expected stream creation/resume response'
        end

        @mutex.synchronize do
          raise ClosedStreamError, 'stream closed' if @stop || @error
          apply_handshake_locked(kind, value)
          @wire_next = @persistent ? @next_offset : 0
          @wire_map = {}
          @last_wire_ack = -1
          @highest_wire_sent = -1
          @last_ack_progress = monotonic
          @pending.each_value { |entry| entry.wire = nil; entry.sent_at = nil }
          @unsent = @pending.values
          @rotation_deadline = nil
          @state = :open
          @ready = true
          @condition.broadcast
        end
        break
      end
    end

    def active_loop
      loop do
        break if stopped?

        work = @mutex.synchronize do
          if @rotation_deadline
            sent_pending = !@wire_map.empty?
            raise ZerobusException, 'server requested stream rotation' if !sent_pending || monotonic >= @rotation_deadline

            []
          else
            @last_ack_progress = monotonic if @wire_map.empty?
            @unsent.shift(64).map do |entry|
              wire = @persistent ? entry.offset : @wire_next
              @wire_next += 1
              entry.wire, entry.sent_at = wire, monotonic
              @wire_map[wire] = entry.offset
              @highest_wire_sent = wire
              [entry, wire]
            end
          end
        end

        work.each do |entry, wire|
          @transport.send_message(ingest_message(entry, wire))
        end

        message = @transport.next_message(timeout: 0.02)
        handle_message(message) if message

        if @transport.respond_to?(:completed?) && @transport.completed?
          raise ZerobusException, 'server ended gRPC stream'
        end

        @mutex.synchronize do
          if !@wire_map.empty? && monotonic - @last_ack_progress > options.server_lack_of_ack_timeout_ms / 1000.0
            raise TimeoutError, 'server acknowledgment timed out'
          end
        end
      end
    end

    def handle_message(message)
      kind, value = decode_response(message)

      @mutex.synchronize do
        # Terminal callbacks have already been published. A transport read that
        # finishes during close must not ACK those submissions a second time.
        return if @stop || @error

        case kind
        when :ack
          raise NonRetriableException, 'acknowledgment exceeds submitted offset' if value > @highest_wire_sent

          if value > @last_wire_ack
            logical = nil
            while @wire_map.first && @wire_map.first[0] <= value
              _, logical = @wire_map.shift
            end
            @last_wire_ack = value
            @last_ack_progress = monotonic
            acknowledge_locked(logical) if logical
          end
        when :rotate
          cap = options.stream_paused_max_wait_time_ms
          seconds = cap.nil? ? value : [value, cap / 1000.0].min
          @rotation_deadline = monotonic + [seconds, 0].max
          @state = :paused
        when :unknown # Forward-compatible unknown service messages.
        else raise NonRetriableException, 'unexpected stream handshake response'
        end
      end
    end

    def acknowledge_locked(offset)
      return if offset <= @acked_offset

      @acked_offset = offset
      while @pending.first && @pending.first[0] <= offset
        logical, entry = @pending.shift
        @inflight_used -= @persistent ? 1 : entry.records.size
        # Admission reserves a slot through callback completion, so a cumulative
        # ACK can enqueue every event without waiting while holding @mutex.
        @callbacks.push([:ack, logical], true) if options.ack_callback
      end
      @condition.broadcast
    end

    def terminal_locked(error)
      return if @error

      @error = error
      @state = :failed
      if options.ack_callback
        @pending.each_key { |offset| @callbacks.push([:error, offset, error.message], true) }
      end
      @condition.broadcast
    end

    def run_callbacks
      while (event = @callbacks.pop)
        failure = nil
        begin
          kind, offset, message = event
          kind == :ack ? options.ack_callback.on_ack(offset) : options.ack_callback.on_error(offset, message)
        rescue StandardError => error
          failure = error
        ensure
          @mutex.synchronize do
            if failure
              @callback_errors.shift if @callback_errors.length == options.callback_queue_capacity
              @callback_errors << failure
            end
            @pending_callbacks -= 1
            @condition.broadcast
          end
        end
      end
    end
  end

  class PersistentStream < ZerobusStream; end
end
