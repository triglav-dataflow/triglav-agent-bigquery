# frozen_string_literal: true

require 'triglav/agent/vertica/monitor'
require_relative 'helper'
require_relative 'support/create_table'

# This test requires a real connection to vertica, now
# Configure .env to set proper connection_info of test/config.yml
#
# TRIGLAV_URL=http://localhost:7800
# TRIGLAV_USERNAME=triglav_test
# TRIGLAV_PASSWORD=triglav_test
# VERTICA_HOST=vertica
# VERTICA_PORT=5433
# VERTICA_DATABASE=vdb
# VERTICA_USER=dbwrite
# VERTICA_PASSWORD=xxxxxxx
if File.exist?(File.join(ROOT, '.env'))
  class TestMonitor < Test::Unit::TestCase
    include CreateTable
    Monitor = Triglav::Agent::Vertica::Monitor

    class << self
      def startup
        Timecop.travel(Time.parse("2016-12-30 23:00:00 +09:00"))
        create_table
        insert_data
      end

      def shutdown
        drop_table
        Timecop.return
      end
    end

    def build_resource(params = {})
      TriglavClient::ResourceResponse.new({
        uri: "vertica://#{host}:#{port}/#{db}/#{schema}/#{table}",
        unit: 'daily',
        timezone: '+09:00',
        span_in_days: 2,
        consumable: true,
        notifiable: false,
      }.merge(params))
    end

    def test_process
      resource = build_resource
      monitor = Monitor.new(connection, resource)
      assert_nothing_raised { monitor.process }
    end

    def test_get_hourly_events
      resource = build_resource(unit: 'hourly')
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == resource.span_in_days * 24 }
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
      monitor = Monitor.new(connection, resource, last_epoch: 0)
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
      monitor = Monitor.new(connection, resource, last_epoch: 0)
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

    def test_get_daily_hourly_events
      resource = build_resource(unit: 'daily,hourly')
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == resource.span_in_days * 24 + resource.span_in_days }
        assert { events.first[:resource_unit] == 'hourly' }
        assert { events.last[:resource_unit] == 'daily' }
      end
      assert { success }
    end

    def test_get_singular_daily_hourly_events
      resource = build_resource(unit: 'singular,daily,hourly')
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == resource.span_in_days * 24 + resource.span_in_days + 1 }
        assert { events.any? {|e| e[:resource_unit] == 'hourly' } }
        assert { events.any? {|e| e[:resource_unit] == 'daily' } }
        assert { events.any? {|e| e[:resource_unit] == 'singular' } }
      end
      assert { success }
    end

    def test_query_date
      resource = build_resource(
        uri: "vertica://#{host}:#{port}/#{db}/#{schema}/#{table}?date=date"
      )
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      q_date = monitor.send(:q_date)
      assert { q_date == %Q["date"] }
    end

    def test_query_timestamp
      resource = build_resource(
        uri: "vertica://#{host}:#{port}/#{db}/#{schema}/#{table}?timestamp=timestamp"
      )
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      q_timestamp = monitor.send(:q_timestamp)
      assert { q_timestamp == %Q["timestamp"] }
    end

    # Value specification:
    # * A value looks like an integer string is treated as an integer.
    # * If you want to treat it as as string, surround with double quote or single quote.
    # * A value does not look like an integer is treated as a string.
    # Operator specification:
    # * Only equality is supported now
    def test_query_where
      resource = build_resource(
        uri: "vertica://#{host}:#{port}/#{db}/#{schema}/#{table}?where[id]=0&where[uuid]='0'&where[d]=2016-12-30"
      )
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      q_where = monitor.send(:q_where)
      assert { q_where == %Q["id" = 0 AND "uuid" = '0' AND "d" = '2016-12-30'] }
    end

    def test_get_events_with_query_where
      resource = build_resource(
        unit: 'singular,daily,hourly',
        uri: "vertica://#{host}:#{port}/#{db}/#{schema}/#{table}?where[id]=0&where[uuid]='0'"
      )
      monitor = Monitor.new(connection, resource, last_epoch: 0)
      success = monitor.process do |events|
        assert { events != nil}
        assert { events.size == 3 }
        assert { events.any? {|e| e[:resource_unit] == 'hourly' } }
        assert { events.any? {|e| e[:resource_unit] == 'daily' } }
        assert { events.any? {|e| e[:resource_unit] == 'singular' } }
      end
      assert { success }
    end
  end
end
