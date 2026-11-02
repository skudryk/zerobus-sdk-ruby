# frozen_string_literal: true
require 'zerobus-sdk-ruby-sdk'
sdk = Zerobus::ZerobusSdk.new(host: ENV.fetch('ZEROBUS_ENDPOINT'),
                             unity_catalog_url: ENV.fetch('DATABRICKS_WORKSPACE_URL'))
stream = sdk.create_stream(client_id: ENV.fetch('DATABRICKS_CLIENT_ID'),
                           client_secret: ENV.fetch('DATABRICKS_CLIENT_SECRET'),
                           table_properties: Zerobus::TableProperties.new(ENV.fetch('ZEROBUS_TABLE')))
begin
  10.times { |n| stream.ingest_record_offset({'value' => n}) }
  stream.flush
ensure
  stream.close
end
