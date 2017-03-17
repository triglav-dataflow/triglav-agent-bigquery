require 'triglav/agent/base/monitor'
require 'uri'
require 'cgi'
require 'securerandom'

module Triglav::Agent
  module Bigquery
    class Monitor < Base::Monitor
      attr_reader :connection, :resource_uri_prefix, :resource

      # @param [Triglav::Agent::Bigquery::Connection] connection
      # @param [String] resource_uri_prefix
      # @param [TriglavClient::ResourceResponse] resource
      # resource:
      #   uri: https://bigquery.cloud.google.com/table/project:dataset.table
      #   unit: 'daily', 'hourly', or 'singular'
      #   timezone: '+09:00'
      #   span_in_days: 32
      def initialize(connection, resource_uri_prefix, resource)
        @connection = connection
        @resource_uri_prefix = resource_uri_prefix
        @resource = resource
        @status = Triglav::Agent::Status.new(resource_uri_prefix, resource.uri)
      end

      def process
        unless resource_valid?
          $logger.warn { "Broken resource: #{resource.to_s}" }
          return nil
        end

        $logger.debug { "Start process #{resource.uri}" }

        events, new_last_modified_times = get_events

        $logger.debug { "Finish process #{resource.uri}" }

        return nil if events.nil? || events.empty?
        yield(events) if block_given? # send_message
        update_status_file(new_last_modified_times)
        true
      end

      private

      def last_modified_times
        @last_modified_times ||= get_last_modified_times
      end

      def get_events
        if partitioned_table?
          new_last_modified_times = get_new_last_modified_times_for_partitioned_table
        else
          new_last_modified_times = get_new_last_modified_times_for_non_partitioned_table
        end
        latest_tables = select_latest_tables(new_last_modified_times)
        events = build_events(latest_tables)
        [events, new_last_modified_times]
      rescue => e
        $logger.warn { "#{e.class} #{e.message} #{e.backtrace.join("\n  ")}" }
        nil
      end

      def update_status_file(last_modified_times)
        last_modified_times[:max] = last_modified_times.values.max
        @status.merge!(last_modified_times)
      end

      def get_last_modified_times
        max_last_modified_time = @status.getsetnx([:max], $setting.debug? ? 0 : get_current_time)
        last_modified_times = @status.get
        removes = last_modified_times.keys - tables.keys
        appends = tables.keys - last_modified_times.keys
        removes.each {|table| last_modified_times.delete(table) }
        appends.each {|table| last_modified_times[table] = max_last_modified_time }
        last_modified_times
      end

      def get_current_time
        (Time.now.to_f * 1000).to_i # msec
      end

      def resource_valid?
        self.class.resource_valid?(resource)
      end

      def self.resource_valid?(resource)
        resource_unit_valid?(resource) && !resource.timezone.nil? && !resource.span_in_days.nil?
      end

      # Two or more combinations are not allowed for hdfs because
      # * hourly should have %d, %H
      # * daily should have %d, but not have %H
      # * singualr should not have %d
      # These conditions conflict.
      def self.resource_unit_valid?(resource)
        units = resource.unit.split(',').sort
        return false if units.size >= 2
        if units.include?('hourly')
          return false unless resource.uri.match(/%H/)
        end
        # if units.include?('daily')
        #   return false unless resource.uri.match(/%d/)
        # end
        if units.include?('singular')
          return false if resource.uri.match(/%[YmdH]/)
        end
        true
      end

      def dates
        return @dates if @dates
        now = Time.now.localtime(resource.timezone)
        @dates = resource.span_in_days.times.map do |i|
          (now - (i * 86000)).to_date
        end
      end

      def project_dataset_table
        @project_dataset_table ||= resource.uri.split('/').last
      end

      def project
        @project ||= project_dataset_table.split(':').first
      end

      def dataset
        @dataset ||= project_dataset_table.split(':').last.chomp(".#{table}")
      end

      def table
        @table ||= project_dataset_table.split('.').last
      end

      def partitioned_table?
        table.include?('$')
      end

      def table_without_partition
        @table_without_partition ||= table.split('$').first
      end

      def dates
        return @dates if @dates
        now = Time.now.localtime(resource.timezone)
        @dates = resource.span_in_days.times.map do |i|
          (now - (i * 86000)).to_date
        end
      end

      def tables
        return @tables if @tables
        tables = {}
        # If table becomes same, use newer date
        case resource.unit
        when 'hourly'
          dates.each do |date|
            date_time = date.to_time
            (0..23).each do |hour|
              _table = (date_time + hour * 3600).strftime(table)
              tables[_table.to_sym] = [date, hour]
            end
          end
        when 'daily'
          hour = 0
          dates.each do |date|
            _table = date.strftime(table)
            tables[_table.to_sym] = [date, hour]
          end
        when 'singular'
          tables[table.to_sym] = [nil, nil]
        end
        @tables = tables
      end

      def get_new_last_modified_times_for_partitioned_table
        rows = connection.get_partitions_summary(
          project: project, dataset: dataset, table: table_without_partition, limit: resource.span_in_days
        )
        new_last_modified_times = {}
        rows.each do |partition, creation_time, last_modified_time|
          new_last_modified_times["#{table_without_partition}$#{partition}".to_sym] = last_modified_time
        end
        new_last_modified_times
      end

      def get_new_last_modified_times_for_non_partitioned_table
        new_last_modified_times = {}
        tables.each do |table, date_hour|
          begin
            result = connection.get_table(project: project, dataset: dataset, table: table)
            new_last_modified_times[table.to_sym] = result[:last_modified_time]
          rescue Connection::NotFoundError => e
            $logger.debug { "#{project}:#{dataset}.#{table.to_s} #=> does not exist" }
          rescue Connection::Error => e
            $logger.warn { "#{project}:#{dataset}.#{table.to_s} #=> #{e.class} #{e.message}" }
          end
        end
        new_last_modified_times
      end

      def select_latest_tables(new_last_modified_times)
        new_last_modified_times.select do |table, last_modified_time|
          is_newer = last_modified_time > (last_modified_times[table] || 0)
          $logger.debug { "#{project}:#{dataset}.#{table} #=> latest_modified_time:#{last_modified_time}, is_newer:#{is_newer}" }
          is_newer
        end
      end

      def build_events(latest_tables)
        latest_tables.map do |table, last_modified_time|
          date, hour = date_hour = tables[table]
          {
            uuid: SecureRandom.uuid,
            resource_uri: resource.uri,
            resource_unit: resource.unit,
            resource_time: date_hour_to_i(date, hour, resource.timezone),
            resource_timezone: resource.timezone,
            payload: {table: table.to_sym, last_modified_time: last_modified_time}.to_json, # msec
          }
        end
      end

      def date_hour_to_i(date, hour, timezone)
        return 0 if date.nil?
        Time.strptime("#{date.to_s} #{hour.to_i} #{timezone}", '%Y-%m-%d %H %z').to_i
      end
    end
  end
end
