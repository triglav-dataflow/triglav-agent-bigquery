# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'triglav/agent/bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "triglav-agent-bigquery"
  spec.version       = Triglav::Agent::Bigquery::VERSION
  spec.authors       = ["Triglav Team"]
  spec.email         = ["triglav_admin_my@dena.jp"]

  spec.summary       = %q{BigQuery agent for triglav, data-driven workflow tool.}
  spec.description   = %q{BigQuery agent for triglav, data-driven workflow tool.}
  spec.homepage      = "https://github.com/triglav-dataflow/triglav-agent-bigquery"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "triglav-agent"
  spec.add_dependency "triglav_client"
  spec.add_dependency "google-api-client"
  spec.add_dependency "ini_file"

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "test-unit-rr"
  spec.add_development_dependency "test-unit-power_assert"
  spec.add_development_dependency "timecop"
end
