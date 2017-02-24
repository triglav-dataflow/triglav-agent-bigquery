# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'triglav/agent/vertica/version'

Gem::Specification.new do |spec|
  spec.name          = "triglav-agent-vertica"
  spec.version       = Triglav::Agent::Vertica::VERSION
  spec.authors       = ["Naotoshi Seo"]
  spec.email         = ["sonots@gmail.com"]

  spec.summary       = %q{Triglav Agent for Vertica.}
  spec.description   = %q{Triglav Agent for Vertica.}
  spec.homepage      = "https://github.com/triglav-dataflow/triglav-agent-vertica"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "vertica"
  spec.add_dependency "triglav-agent"
  spec.add_dependency "triglav_client"
  spec.add_dependency "rack" # Rack::Utils

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "test-unit-rr"
  spec.add_development_dependency "test-unit-power_assert"
  spec.add_development_dependency "timecop"
end
