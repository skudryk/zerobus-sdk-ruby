# frozen_string_literal: true
require_relative 'lib/zerobus/version'
Gem::Specification.new do |spec|
  spec.name = 'zerobus-sdk-ruby'
  spec.version = Zerobus::VERSION
  spec.authors = ['Serhii Kudryk']
  spec.summary = 'Pure Ruby streaming ingestion into Databricks Zerobus'
  spec.description = 'Ruby libcurl ingestion client with OAuth, JSON, protobuf, durable acknowledgments and stream recovery.'
  spec.homepage = 'https://github.com/cropio/zerobus-sdk-ruby'
  spec.license = 'Apache-2.0'
  spec.required_ruby_version = '>= 3.1'
  spec.files = Dir.chdir(__dir__) { Dir['lib/**/*.rb', 'proto/**/*.proto', 'README.md', 'LICENSE', 'NOTICE', 'examples/*.rb'] }
  spec.require_paths = ['lib']
  spec.metadata['source_code_uri'] = spec.homepage
  spec.add_dependency 'typhoeus', '~> 1.7'
  spec.add_dependency 'ethon', '~> 0.19.1'
  spec.add_dependency 'base64', '>= 0.1', '< 1.0'
end
