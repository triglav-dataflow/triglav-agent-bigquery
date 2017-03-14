# frozen_string_literal: true

require 'triglav/agent/bigquery/connection'
require_relative 'support/create_table'
require_relative 'helper'

# This script requires a real connection to bigquery, now
# Configure .env to set proper connection_info of test/support/config.yml
#
# TRIGLAV_URL=http://localhost:7800
# TRIGLAV_USERNAME=triglav_test
# TRIGLAV_PASSWORD=triglav_test
# GOOGLE_APPLICATION_CREDENTIALS: "~/.config/gcloud/application_default_credentials.json"
# GOOGLE_PROJECT: xxx-xxx-xxx
if File.exist?(File.join(ROOT, '.env'))
  class TestMonitor < Test::Unit::TestCase
    include CreateTable
    Connection = Triglav::Agent::Bigquery::Connection

    class << self
      def startup
        Timecop.travel(Time.parse("2017-03-07 23:00:00 +09:00"))
        setup_tables
      end

      def shutdown
        Timecop.return
        teardown_tables
      end
    end

    def test_get_table
      result = connection.get_table(dataset: dataset, table: "#{table}_20170306")
      assert { result.keys == %i[id creation_time last_modified_time location num_bytes num_rows] }
      assert { result[:creation_time].is_a?(Integer) }
      assert { result[:last_modified_time].is_a?(Integer) }
    end

    def test_get_partitions_summary
      result = connection.get_partitions_summary(dataset: dataset, table: partitioned_table, limit: 32)
      assert { result.is_a?(Array) }
      assert { result.first[0].is_a?(String) }
      assert { result.first[1].is_a?(Integer) }
      assert { result.first[2].is_a?(Integer) }
    end
  end
end
