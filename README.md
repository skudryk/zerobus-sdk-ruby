# Databricks ZeroBus Ruby SDK

A standalone Ruby SDK for Databricks Zerobus gem.
It was ported from official Databricks Python SDK (https://github.com/databricks/zerobus-sdk).

All SDK logic is Ruby based, ingestion and OAuth use **Typhoeus and libcurl**.

Typhoeus uses Ethon/FFI and a system libcurl, so the complete dependency stack
includes native libraries. Each stream owns a libcurl multi/easy pair.


Zerobus is a high-throughput streaming service for direct data ingestion into Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.



## Install and build

Requires Ruby 3.1+ and libcurl with HTTP/2 support (`curl --version` should list
`HTTP2`; the SDK checks the library actually loaded by Ethon). On Debian/Ubuntu,
install `libcurl4` and CA certificates. Use TLS endpoints in production. Plain
HTTP with HTTP/2 prior knowledge is available for local testing.


## Prerequisites

Before using any SDK, you need the following:

### 1. Workspace URL and Workspace ID

After logging into your Databricks workspace, look at the browser URL:

```
https://<databricks-instance>.cloud.databricks.com/o=<workspace-id>
```

- **Workspace URL**: The part before `/o=` (e.g., `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`)
- **Workspace ID**: The part after `/o=` (e.g., `1234567890123456`)

> **Note:** The examples above show AWS endpoints (`.cloud.databricks.com`). For Azure deployments, the workspace URL will be `https://<databricks-instance>.azuredatabricks.net`.

### 2. Create a Delta Table

Create a table using Databricks SQL:

```sql
CREATE TABLE <catalog_name>.default.<table_name> (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Replace `<catalog_name>` with your catalog name (e.g., `main`).

### 3. Create a Service Principal

1. Navigate to **Settings > Identity and Access** in your Databricks workspace
2. Click **Service principals** and create a new service principal
3. Generate a new secret for the service principal and save it securely
4. Grant the following permissions:
   - `USE_CATALOG` on the catalog (e.g., `main`)
   - `USE_SCHEMA` on the schema (e.g., `default`)
   - `MODIFY` and `SELECT` on the table

Grant permissions using SQL:

```sql
-- Grant catalog permission
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-application-id>`;

-- Grant schema permission
GRANT USE SCHEMA ON SCHEMA <catalog_name>.default TO `<service-principal-application-id>`;

-- Grant table permissions
GRANT SELECT, MODIFY ON TABLE <catalog_name>.default.<table_name> TO `<service-principal-application-id>`;
```

The service principal's **Application ID** is your OAuth **Client ID**, and the generated secret is your **Client Secret**.

## Ingestion APIs

Pick the record format that matches your data.

- JSON: schema-free ingestion. Pass a JSON string or a native object (dict, map, and so on) and the SDK serializes it. No compilation step. Good for getting started or dynamic schemas.
- Protocol Buffers: strongly-typed, schema-validated ingestion. More compact on the wire than JSON. A typical choice for production workloads that are not already producing Arrow.


## JSON ingestion

```ruby
require 'zerobus-sdk-ruby-sdk'

sdk = Zerobus::ZerobusSdk.new(
  host: ENV.fetch('ZEROBUS_ENDPOINT'),
  unity_catalog_url: ENV.fetch('DATABRICKS_WORKSPACE_URL'),
  application_name: 'my-app/1.0'
)

stream = sdk.create_stream(
  client_id: ENV.fetch('DATABRICKS_CLIENT_ID'),
  client_secret: ENV.fetch('DATABRICKS_CLIENT_SECRET'),
  table_properties: Zerobus::TableProperties.new('catalog.schema.events')
)
begin
  offset = stream.ingest_record_offset({'event_id' => 'one', 'value' => 42})
  last_offset = stream.ingest_records_offset([
    '{"event_id":"two","value":43}',
    '{"event_id":"three","value":44}'
  ])
  stream.wait_for_offset(last_offset)
  stream.flush
ensure
  stream.close
end
```

A submission returns as soon as it is queued, subject to inflight backpressure.
`wait_for_offset` and `flush` confirm **durability**, not just transmission.

Each single submission or entire batch receives one logical offset, starting at
zero. An empty batch returns `nil`. JSON accepts object strings or Ruby hashes;
batches accept enumerables. Validate and encode the entire batch before queuing.

`ingest_record` returns a compatibility `RecordAcknowledgment` with `offset` and
`wait_for_ack`. The `*_nowait` methods queue synchronously and return `nil`;
unlike the Python detached-task methods, validation errors remain observable
and a following `flush` includes the submission.

`close` closes admission, waits for outstanding durability up to `flush_timeout_ms`,
then releases the transport. It raises if data could not be confirmed. After a
failed close, `get_unacked_records` or `get_unacked_batches` returns retained
encoded payloads. `sdk.recreate_stream(closed_stream)` queues them on a new
stream; ephemeral recovery and recreation provide **at-least-once** delivery,
so uncertain commits can produce duplicates.

## Formats

The gem supports JSON and Protobuf records. The format is inferred from
`TableProperties`:

| Properties | Input | Format |
| --- | --- | --- |
| Table name only | JSON object string or Hash | JSON |
| `TableProperties.new(name, descriptor_bytes)` | Serialized bytes or an object responding to `to_proto` | Protobuf |

Protobuf descriptors must be serialized **FileDescriptorProto**, not a
FileDescriptorSet. Generate these with your protobuf tooling. The gem treats
encoded records as opaque binary strings, so no particular protobuf runtime is
required. The Python CLI schema generator is not included. For example, extract the desired file descriptor from a
`protoc --descriptor_set_out=...` output using your application's protobuf library.

Protobuf is a binary encoding, not payload compression. JSON records remain JSON
text inside the Protobuf request envelope.

### Type mappings

Delta column types map to Arrow and proto2 as shown below. JSON is not a typed mapping: objects use the same logical types as the table, and the SDK serializes them without a compiled schema.

For Protocol Buffers, declare fields `optional` when the Delta column is nullable.

| Delta Type | Arrow Type | Proto2 Type |
| --- | --- | --- |
| TINYINT, BYTE | `Int8` | `int32` |
| SMALLINT, SHORT | `Int16` | `int32` |
| INT | `Int32` | `int32` |
| BIGINT, LONG | `Int64` | `int64` |
| FLOAT | `Float32` | `float` |
| DOUBLE | `Float64` | `double` |
| STRING, VARCHAR | `LargeUtf8` | `string` |
| BOOLEAN | `Boolean` | `bool` |
| BINARY | `LargeBinary` | `bytes` |
| DECIMAL | `LargeUtf8` | `string` |
| DATE | `Date32` | `int32` (days since Unix epoch) |
| TIMESTAMP | `Timestamp(Microsecond, UTC)` | `int64` (microseconds since Unix epoch) |
| TIMESTAMP_NTZ | `Timestamp(Microsecond)` (no timezone) | `int64` (microseconds since Unix epoch) |
| ARRAY\<type\> | `List` (item field `item`) | `repeated` type |
| MAP\<key, value\> | `Map` (entries field `entries` with `keys` and `values`) | `map<key, value>` |
| STRUCT\<fields\> | nested struct | nested message |
| VARIANT | struct of `metadata` and `value`, both non-null `LargeBinary` | `string` (JSON) |

DECIMAL is encoded as text on both paths today (`LargeUtf8` / `string`). STRING maps to Arrow `LargeUtf8`, not `Utf8`.

### Acknowledgments and throughput

Ingestion is asynchronous in every SDK. An `ingest` call returns as soon as the record is queued — the SDK sends it and tracks its acknowledgment on a background task. To confirm that records were durably committed, call `flush()`; it returns once everything queued so far has been acknowledged.

The idiomatic flow is therefore **ingest in a loop, then `flush()`** — once at the end of a bounded batch, or periodically for a long-running stream. Where the SDK supports it, you can instead register an ack callback and be notified as records commit, without blocking at all.

Each `ingest` also returns the record's offset, and `wait_for_offset(offset)` blocks until that offset is acknowledged. That's useful when a particular record must be confirmed before you continue; because acknowledgments are ordered, waiting on the last offset of a run confirms the whole run. The one thing to avoid is waiting on every record inside a tight loop — that turns the asynchronous pipeline into a synchronous request/response and limits throughput to a single record per network round-trip.

See each SDK's README for exact method names and a runnable example.

## Proxy configuration (libcurl)

With the default Typhoeus configuration, this Ruby SDK delegates proxy selection
to libcurl for both gRPC ingestion and OAuth token requests. The request URL's
scheme determines which variable applies; using gRPC over HTTPS still selects
`https_proxy`. See [libcurl's environment-variable documentation](https://curl.se/libcurl/c/libcurl-env.html).

| Request URL | Proxy lookup order (first nonempty value) |
| --- | --- |
| `https://` (Zerobus and workspace endpoints) | `https_proxy`, `HTTPS_PROXY`, `all_proxy`, `ALL_PROXY` |
| `http://` (local development) | `http_proxy`, `all_proxy`, `ALL_PROXY` |

Lowercase names take precedence over uppercase names. On systems with
case-sensitive environment variables, including Linux and macOS, `HTTP_PROXY` is
ignored; use `http_proxy`. `http_proxy` is not a fallback for an HTTPS URL.
Scheme-specific settings take precedence over `all_proxy` / `ALL_PROXY`.

The variables `grpc_proxy`, `GRPC_PROXY`, `no_grpc_proxy`, and `NO_GRPC_PROXY` are
**not supported** by this SDK. A deployment using them must migrate to the
libcurl variables above. There is no gRPC-core proxy-variable precedence layer.

For bypasses, libcurl checks `no_proxy` before `NO_PROXY`. Its comma-separated
list accepts hostnames and IP addresses. A domain entry such as `example.com`
matches that host and its subdomains, but not `otherexample.com`. Set the value
to `*` to bypass proxies for all hosts; patterns such as `*.example.com` are not
supported. IPv6 literals are written without brackets. IP network ranges in CIDR
notation require libcurl 7.86.0 or later. See [libcurl's bypass rules](https://curl.se/libcurl/c/CURLOPT_NOPROXY.html).

```bash
export https_proxy=http://my-proxy:8080
export no_proxy=localhost,127.0.0.1,::1
```

For HTTPS destinations through an HTTP proxy, libcurl uses an HTTP CONNECT tunnel
and then establishes TLS to the destination, with certificate and hostname
verification enabled. Other proxy schemes depend on the installed libcurl build;
see [libcurl's proxy documentation](https://curl.se/libcurl/c/CURLOPT_PROXY.html).
These settings apply to the Zerobus endpoint and the workspace token endpoint
independently, so the bypass list should cover each destination that needs a
direct connection.

## Authentication

OAuth client credentials are exchanged at the workspace `/oidc/v1/token`
endpoint using the same table-scoped Unity Catalog authorization details and
workspace resource audience as the official Databricks Zerobus SDK do. 

Tokens are cached per SDK, identity and table, refreshed on access near expiry, and invalidated after a
gRPC unauthenticated response. A transient proactive refresh failure can reuse a
still-valid cached token. Redirects are disabled and TLS certificates and
hostnames are verified.

External identity-provider federation is available for ephemeral row streams:

```ruby
auth = Zerobus::FederatedToken.new(
  idp_token_supplier: -> { current_external_jwt },
  databricks_client_id: 'service-principal-id' # omit for account federation
)
stream = sdk.create_stream(table_properties: props, auth: auth)
```

The supplier must return a String and should return a cached token promptly.
For custom authentication, pass a callable returning a header Hash, or an object
implementing `get_headers` and optionally `invalidate`:

```ruby
provider = -> { {'authorization' => "Bearer #{current_token}"} }
stream = sdk.create_stream(table_properties: props, headers_provider: provider)
```

The SDK supplies `x-databricks-zerobus-table-name` and its user agent. Custom
headers providers override client credentials, as in Databricks ZeroBus Python SDK. 

Federation cannot be combined with client credentials.

## Options and callbacks

`StreamConfigurationOptions.new` configures row-stream behavior:

| Option | Default |
| --- | --- |
| `max_inflight_records` | 1,000,000 |
| `recovery` | `true` |
| `recovery_timeout_ms` | 15,000 |
| `recovery_backoff_ms` | 2,000 |
| `recovery_retries` | 4 |
| `server_lack_of_ack_timeout_ms` | 60,000 |
| `flush_timeout_ms` | 300,000 |
| `stream_paused_max_wait_time_ms` | `nil` (server grace period) |
| `callback_max_wait_time_ms` | 5,000 (`nil` waits indefinitely) |
| `ack_callback` | `nil` |
| `callback_queue_capacity` | 1,000 (positive integer) |
| `record_type` | `RecordType::UNSPECIFIED` (infer from properties) |
| `max_ingest_payload_bytes` | 10 MiB minus 64 KiB |

A specified `record_type` must match the inferred format. Inflight capacity counts
individual row records on ephemeral streams and submissions on persistent streams.
The encoded byte limit applies to one submission; oversized submissions are
rejected before allocating an offset. Recovery retries are bounded over the
stream lifetime in this initial port. Stream-creation waiting is bounded by
`recovery_timeout_ms`; user-supplied authentication callbacks must return promptly.

```ruby
class Callback < Zerobus::AckCallback
  def on_ack(offset)
    puts "Durable: #{offset}"
  end

  def on_error(offset, message)
    warn "Failed: #{offset}: #{message}"
  end
end
options = Zerobus::StreamConfigurationOptions.new(
  ack_callback: Callback.new, callback_queue_capacity: 1000
)
```

Callbacks run in order on a separate thread, once per accepted submission,
including one callback for a whole batch. Rejected inputs generate no callback.
The callback queue is bounded and uses **blocking admission with reserved slots**:

- Each accepted submission reserves one callback slot before receiving an offset.
  The reservation covers time awaiting its ACK, waiting in the callback queue,
  and executing its callback.
- The slot is released only when `on_ack` or `on_error` returns or raises. Server
  ACKs still release record inflight capacity immediately, but cannot release the
  callback reservation.
- Once `callback_queue_capacity` slots are reserved, ingestion waits in the caller
  for a callback to finish, up to `flush_timeout_ms`. Timeout rejects that new
  submission before allocating an offset; it generates no callback.
- ACK processing and terminal-error delivery never wait for callback queue space
  while holding the stream mutex. Every accepted submission already owns a slot;
  callback events are neither dropped nor coalesced.

This limit applies only when `ack_callback` is set. It is separate from
`max_inflight_records`; a multi-record batch still reserves just one callback slot.
Persistent streams expose the same setting through `.callback_queue_capacity(n)`.
`flush` and `wait_for_offset` confirm durability and do not wait for callbacks.

`close` stops callback admission and closes the queue after publishing all accepted
submissions' events. The callback worker drains those events in order. The
`callback_max_wait_time_ms` budget limits how long close waits for that drain;
expiration does not discard events or stop the worker. Delivery can continue after
close returns while the process remains running. Use `nil` to wait indefinitely.

Callback exceptions do not fail ingestion or strand capacity. `stream.callback_errors`
returns and clears retained exception diagnostics. To bound memory even when every
callback raises, only the most recent `callback_queue_capacity` exceptions are
retained; older diagnostics are evicted without skipping callback invocations.
Callbacks may query or close their stream. A synchronous submission from a callback
on the same stream raises immediately if it would have to wait for capacity,
avoiding a wait on its own completion. A callback should not wait on an async
submission to its own stream, since that operation can need the callback slot
still held by the callback. Callback code should finish promptly.

## Persistent streams (experimental)

Persistent streams use the canonical service's experimental durable-stream API.
They support JSON and protobuf, with OAuth or a custom headers provider.
Persist the server-issued stream ID and coordinate application checkpoints with
the server's committed watermark.

```ruby
builder = sdk.persistent_stream_builder
  .table('catalog.schema.events')
  .oauth(ENV.fetch('DATABRICKS_CLIENT_ID'), ENV.fetch('DATABRICKS_CLIENT_SECRET'))
  .json
  .max_inflight_requests(1000)

stream = builder.build
saved_id = stream.stream_id
stream.ingest_record_offset('{"event_id":"one"}')
stream.close

resumed = builder.resume(saved_id)
puts resumed.resume_watermark # -1 when no record has committed
# The next submission starts at resume_watermark + 1.
resumed.close

sdk.retire_stream(table_name: 'catalog.schema.events', stream_id: saved_id,
                 headers_provider: provider)
```

A reconnect resumes the same ID, removes submissions already committed according
to the watermark, and replays the remainder with their original offsets. Retirement
is permanent and idempotent on the server. Exactly-once behavior depends on the
server's persistent-stream contract and consistent application checkpointing.

## Asynchronous API

`Zerobus::Async::ZerobusSdk` exposes ingestion operations through `Future` objects.
`.value` (or `.wait`) returns the result or raises the original exception. Each
async SDK owns a fixed worker pool shared by its streams, persistent builders,
recreation and retirement operations. By default it has **4 workers** and a queue
for **1,000 waiting operations**. Set `worker_count` and `queue_capacity` to positive
integers when constructing the SDK to change those limits.

When the queue is full, submitting another operation blocks the calling thread
until a queue slot becomes available. Submission therefore applies backpressure
before returning a future, rather than creating another thread to wait for stream
capacity. `max_inflight_records` still limits records admitted to the stream;
`queue_capacity` separately limits operations waiting for an async worker. Concurrent
operations may be admitted to a stream in a different order from their submission.

```ruby
sdk = Zerobus::Async::ZerobusSdk.new(
  host: endpoint, unity_catalog_url: workspace,
  worker_count: 4, queue_capacity: 1000
)
begin
  stream = sdk.create_stream(table_properties: props, headers_provider: provider).value
  begin
    offset = stream.ingest_record_offset('{"value":42}').value
    stream.wait_for_offset(offset).value
  ensure
    stream.close.value
  end
ensure
  sdk.shutdown
end
```

Close streams before calling `sdk.shutdown`. Shutdown stops accepting work, wakes
callers blocked on a full queue with `Zerobus::Async::ExecutorShutdownError`, drains
accepted operations, and waits for the workers to exit. `sdk.shutdown(wait: false)`
initiates the same drain without waiting for worker exit. It does not close streams
or cancel running operations; outstanding waits retain their stream timeouts.

`future.value(timeout: seconds)` bounds only the caller's wait; it does not cancel
the queued work or bound submission time when the work queue is full. A future
stores its result independently of the worker, so multiple callers may wait for
it, and task failures do not terminate workers.

Standalone `Zerobus::Future.new { ... }` calls share a process-wide bounded pool
with the same defaults. For explicit lifetime control, create a
`Zerobus::Async::Executor`, pass it as `Future.new(executor: executor)`, and call
`executor.shutdown` when finished. A future created inside a worker of the same
executor runs on that worker to avoid waiting for capacity in its own pool.
