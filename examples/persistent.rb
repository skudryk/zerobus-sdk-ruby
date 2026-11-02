# frozen_string_literal: true
require 'zerobus-sdk-ruby-sdk'
sdk = Zerobus::ZerobusSdk.new(host: ENV.fetch('ZEROBUS_ENDPOINT'),
                             unity_catalog_url: ENV.fetch('DATABRICKS_WORKSPACE_URL'))
builder = sdk.persistent_stream_builder.table(ENV.fetch('ZEROBUS_TABLE'))
             .oauth(ENV.fetch('DATABRICKS_CLIENT_ID'), ENV.fetch('DATABRICKS_CLIENT_SECRET')).json
stream = ENV['ZEROBUS_STREAM_ID'] ? builder.resume(ENV.fetch('ZEROBUS_STREAM_ID')) : builder.build
puts "Stream ID: #{stream.stream_id}; resume watermark: #{stream.resume_watermark.inspect}"
begin
  stream.ingest_record_offset({'value' => 42})
  stream.flush
ensure
  stream.close
end
