# frozen_string_literal: true

require 'triglav/agent/bigquery/monitor'
require_relative 'helper'
require_relative 'support/create_table'
require 'fileutils'

# This test requires a real connection to bigquery, now
# Configure .env to set proper connection_info of test/config.yml
#
# TRIGLAV_URL=http://localhost:7800
# TRIGLAV_USERNAME=triglav_test
# TRIGLAV_PASSWORD=triglav_test
# GOOGLE_APPLICATION_CREDENTIALS: "~/.config/gcloud/application_default_credentials.json"
# GOOGLE_PROJECT: xxx-xxx-xxx
if File.exist?(File.join(ROOT, '.env'))
  class TestMonitor < Test::Unit::TestCase
    include CreateTable
    Monitor = Triglav::Agent::Bigquery::Monitor

    class << self
      def startup
        Timecop.travel(Time.parse("2017-03-07 23:00:00 +09:00"))
        FileUtils.rm_f($setting.status_file)
        setup_tables
      end

      def shutdown
        teardown_tables
        Timecop.return
      end
    end

    def build_uri(project, dataset, table)
      "https://bigquery.cloud.google.com/table/#{project}:#{dataset}.#{table}"
    end

    def build_resource(params = {})
      unit = params[:unit] || 'daily'
      uri = params[:uri] ||
        case unit
        when /hourly/
          build_uri(project, dataset, "#{table}_%H_%Y%m%d")
        when /daily/
          _table = params.delete(:partitioned) ? "#{partitioned_table}$" : "#{table}_"
          build_uri(project, dataset, "#{_table}%Y%m%d")
        when /singular/
          build_uri(project, dataset, table)
        end
      TriglavClient::ResourceResponse.new({
        uri: uri,
        unit: unit,
        timezone: '+09:00',
        span_in_days: 2,
        consumable: true,
        notifiable: false,
      }.merge(params))
    end

    def resource_uri_prefix
      'https://bigquery.cloud.google.com'
    end

    def test_resource_valid
      resource = build_resource(unit: 'singular,daily,hourly')
      assert { Monitor.resource_valid?(resource) == false }

      resource = build_resource(unit: 'daily,hourly')
      assert { Monitor.resource_valid?(resource) == false }

      # resource = build_resource(unit: 'hourly', uri: build_uri(project, dataset, "#{table}_%Y%m%d"))
      # assert { Monitor.resource_valid?(resource) == false }

      # resource = build_resource(unit: 'daily', uri: build_uri(project, dataset, "#{table}_%Y%m"))
      # assert { Monitor.resource_valid?(resource) == false }

      resource = build_resource(unit: 'singular', uri: build_uri(project, dataset, "#{table}_%Y%m%d"))
      assert { Monitor.resource_valid?(resource) == false }
    end

    def test_project_dataset_table
      resource = build_resource(unit: 'singular')
      monitor = Monitor.new(connection, resource_uri_prefix, resource)
      assert { monitor.send(:project_dataset_table) == "#{project}:#{dataset}.#{table}" }
      assert { monitor.send(:project) == project }
      assert { monitor.send(:dataset) == dataset }
      assert { monitor.send(:table) == table }
    end

    def test_process
      resource = build_resource
      monitor = Monitor.new(connection, resource_uri_prefix, resource)
      assert_nothing_raised { monitor.process }
    end

    def test_get_hourly_events
      resource = build_resource(unit: 'hourly')
      monitor = Monitor.new(connection, resource_uri_prefix, resource)
      success = monitor.process do |events|
        assert { events != nil}
        # assert { events.size == resource.span_in_days * 24 }
        event = events.first
        assert { event.keys == %i[uuid resource_uri resource_unit resource_time resource_timezone payload] }
        assert { event[:resource_uri] == resource.uri }
        assert { event[:resource_unit] == resource.unit }
        assert { event[:resource_timezone] == resource.timezone }
      end
      assert { success }
    end

    def test_get_daily_events
      resource = build_resource(unit: 'daily')
      monitor = Monitor.new(connection, resource_uri_prefix, resource)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == resource.span_in_days }
        event = events.first
        assert { event.keys == %i[uuid resource_uri resource_unit resource_time resource_timezone payload] }
        assert { event[:resource_uri] == resource.uri }
        assert { event[:resource_unit] == resource.unit }
        assert { event[:resource_timezone] == resource.timezone }
      end
      assert { success }
    end

    def test_get_singular_events
      resource = build_resource(unit: 'singular')
      monitor = Monitor.new(connection, resource_uri_prefix, resource)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == 1 }
        event = events.first
        assert { event.keys == %i[uuid resource_uri resource_unit resource_time resource_timezone payload] }
        assert { event[:resource_uri] == resource.uri }
        assert { event[:resource_unit] == resource.unit }
        assert { event[:resource_timezone] == resource.timezone }
      end
      assert { success }
    end

    def test_get_daily_events_for_partitioned_table
      resource = build_resource(unit: 'daily', partitioned: true)
      monitor = Monitor.new(connection, resource_uri_prefix, resource)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == resource.span_in_days }
        event = events.first
        assert { event.keys == %i[uuid resource_uri resource_unit resource_time resource_timezone payload] }
        assert { event[:resource_uri] == resource.uri }
        assert { event[:resource_unit] == resource.unit }
        assert { event[:resource_timezone] == resource.timezone }
      end
      assert { success }
    end
  end
end
