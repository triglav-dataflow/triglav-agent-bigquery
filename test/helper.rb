# frozen_string_literal: true

require 'test/unit'
require 'test/unit/rr'
require 'pry'
require 'timecop'
require 'triglav/agent/vertica'

TEST_ROOT = __dir__
ROOT = File.dirname(__dir__)
ENV['APP_ENV'] = 'test'

opts = {
  config: File.join(TEST_ROOT, 'support', 'config.yml'),
  status: File.join(TEST_ROOT, 'tmp', 'status.yml'),
  token: File.join(TEST_ROOT, 'tmp', 'token.yml'),
  dotenv: true,
  debug: true,
}
$setting = Triglav::Agent::Configuration.setting_class.new(opts)
$logger = $setting.logger
