require 'triglav/agent/base/monitor'
require 'vertica'
require 'uri'
require 'cgi'
require 'securerandom'
require 'rack/utils'

module Triglav::Agent
  module Vertica
    class Monitor < Base::Monitor
      attr_reader :connection, :resource, :periodic_last_epoch, :singular_last_epoch

      # @param [Triglav::Agent::Vertica::Connection] connection
      # @param [TriglavClient::ResourceResponse] resource
      # resource:
      #   uri: vertica://host/database/schema/table
      #   unit: 'daily', 'hourly', 'singular', or their combinations such as 'singular,daily,hourly'
      #   timezone: '+09:00'
      #   span_in_days: 32
      #
      # View is not supported
      def initialize(connection, resource)
        @connection = connection
        @resource = resource
        @periodic_last_epoch = $setting.debug? ? 0 : get_from_status_file(:periodic_last_epoch)
        @singular_last_epoch = $setting.debug? ? 0 : get_from_status_file(:singular_last_epoch)
      end

      def process
        unless resource_valid?
          $logger.warn { "Broken resource: #{resource.to_s}" }
          return nil
        end

        $logger.debug {
          msgs = ["Start process #{resource.uri}"]
          msgs << "periodic_last_epoch:#{periodic_last_epoch}" if periodic_last_epoch
          msgs << "singular_last_epoch:#{singular_last_epoch}" if singular_last_epoch
          msgs.join(', ')
        }

        if periodic?
          periodic_events, new_periodic_last_epoch = get_periodic_events
          events = periodic_events || []
        end
        if singular?
          singular_events, new_singular_last_epoch = get_singular_events
          events.nil? ? (events = singular_events) : events.concat(singular_events || [])
        end

        $logger.debug {
          msgs = ["Finish process #{resource.uri}"]
          msgs << "periodic_last_epoch:#{periodic_last_epoch}" if periodic_last_epoch
          msgs << "singular_last_epoch:#{singular_last_epoch}" if singular_last_epoch
          msgs << "new_periodic_last_epoch:#{new_periodic_last_epoch}" if new_periodic_last_epoch
          msgs << "new_singular_last_epoch:#{new_singular_last_epoch}" if new_singular_last_epoch
          msgs.join(', ')
        }

        return nil if events.nil? || events.empty?
        yield(events) if block_given? # send_message
        update_status_file(:periodic_last_epoch, new_periodic_last_epoch) if new_periodic_last_epoch
        update_status_file(:singular_last_epoch, new_singular_last_epoch) if new_singular_last_epoch
        true
      end

      def get_periodic_events
        if hourly?
          events, new_last_epoch, rows = get_hourly_events
          if daily?
            daily_events = build_daily_events_from_hourly(rows)
            events.concat(daily_events)
          end
          [events, new_last_epoch]
        elsif daily?
          get_daily_events
        else
          raise
        end
      end

      def get_singular_events
        sql = "select " \
          "NULL AS d, NULL AS h, max(epoch) " \
          "from #{q_db}.#{q_schema}.#{q_table} " \
          "#{q_where.empty? ? '' : "where #{q_where} "}" \
          "having max(epoch) > #{q_singular_last_epoch}"
        query_and_get_events(:singular, sql)
      end

      def get_hourly_events
        sql = "select " \
          "#{q_date} AS d, DATE_PART('hour', #{q_timestamp}) AS h, max(epoch) " \
          "from #{q_db}.#{q_schema}.#{q_table} " \
          "where #{q_date} IN ('#{dates.join("','")}') " \
          "#{q_where.empty? ? '' : "AND #{q_where} "}" \
          "group by d, h having max(epoch) > #{q_periodic_last_epoch} " \
          "order by d, h"
        query_and_get_events(:hourly, sql)
      end

      def get_daily_events
        sql = "select " \
          "#{q_date} AS d, 0 AS h, max(epoch) " \
          "from #{q_db}.#{q_schema}.#{q_table} " \
          "where #{q_date} IN ('#{dates.join("','")}') " \
          "#{q_where.empty? ? '' : "AND #{q_where} "}" \
          "group by d having max(epoch) > #{q_periodic_last_epoch} " \
          "order by d"
        query_and_get_events(:daily, sql)
      end

      private

      def query_and_get_events(unit, sql)
        $logger.debug { "Query: #{sql}" }
        rows = connection.query(sql)
        events = build_events(unit, rows)
        new_last_epoch = build_latest_epoch(rows)
        [events, new_last_epoch, rows]
      rescue ::Vertica::Error::QueryError => e
        $logger.warn { "#{e.class} #{e.message}" } # e.message includes sql
        nil
      rescue ::Vertica::Error::TimedOutError => e
        $logger.warn { "#{e.class} #{e.message} SQL:#{sql}" }
        nil
      end

      def update_status_file(key, last_epoch)
        Triglav::Agent::StorageFile.set(
          $setting.status_file,
          [resource.uri.to_sym, key.to_sym],
          last_epoch
        )
      end

      def get_from_status_file(key)
        Triglav::Agent::StorageFile.getsetnx(
          $setting.status_file,
          [resource.uri.to_sym, key.to_sym],
          get_current_epoch
        )
      end

      def get_current_epoch
        connection.query('select GET_CURRENT_EPOCH()').first.first
      end

      def resource_valid?
        resource_unit_valid? && !resource.timezone.nil? && !resource.span_in_days.nil?
      end

      def resource_unit_valid?
        resource.unit.split(',').each do |item|
          return false unless %w[singular daily hourly].include?(item)
        end
        true
      end

      def hourly?
        return @is_hourly unless @is_hourly.nil?
        @is_hourly = resource.unit.include?('hourly')
      end

      def daily?
        return @is_daily unless @is_daily.nil?
        @is_daily = resource.unit.include?('daily')
      end

      def singular?
        return @is_singular unless @is_singular.nil?
        @is_singular = resource.unit.include?('singular')
      end

      def periodic?
        hourly? or daily?
      end

      def dates
        return @dates if @dates
        now = Time.now.localtime(resource.timezone)
        @dates = resource.span_in_days.times.map do |i|
          (now - (i * 86000)).strftime('%Y-%m-%d')
        end
      end

      def build_latest_epoch(rows)
        rows.map {|row| row[2] }.max
      end

      def build_events(unit, rows)
        rows.map do |row|
          date, hour, epoch = row[0], row[1], row[2]
          {
            uuid: SecureRandom.uuid,
            resource_uri: resource.uri,
            resource_unit: unit.to_s,
            resource_time: date_hour_to_i(date, hour, resource.timezone),
            resource_timezone: resource.timezone,
            payload: (date ? {d: date.to_s, h: hour.to_i} : {}).merge!(epoch: epoch).to_json,
          }
        end
      end

      def build_daily_events_from_hourly(rows)
        max_epoch_of = {}
        rows.each do |row|
          date, hour, epoch = row[0], row[1], row[2]
          max_epoch_of[date] = [epoch, max_epoch_of[date] || 0].max
        end
        daily_events = max_epoch_of.map do |date, epoch|
          {
            uuid: SecureRandom.uuid,
            resource_uri: resource.uri,
            resource_unit: 'daily',
            resource_time: date_hour_to_i(date, 0, resource.timezone),
            resource_timezone: resource.timezone,
            payload: {d: date.to_s, h: 0, epoch: epoch}.to_json,
          }
        end
      end

      def date_hour_to_i(date, hour, timezone)
        return 0 if date.nil?
        Time.strptime("#{date.strftime("%Y-%m-%d")} #{hour.to_i} #{timezone}", '%Y-%m-%d %H %z').to_i
      end

      def q_periodic_last_epoch
        @q_periodic_last_epoch ||= ::Vertica.quote(periodic_last_epoch)
      end

      def q_singular_last_epoch
        @q_singular_last_epoch ||= ::Vertica.quote(singular_last_epoch)
      end

      def parsed_uri
        @parsed_uri ||= URI.parse(resource.uri)
      end

      def parsed_query
        @parsed_query ||= Rack::Utils.parse_nested_query(parsed_uri.query)
      end

      def db
        @db ||= parsed_uri.path[1..-1].split('/')[0]
      end

      def schema
        @schema ||= parsed_uri.path[1..-1].split('/')[1]
      end

      def table
        @table ||= parsed_uri.path[1..-1].split('/')[2]
      end

      def date_column
        parsed_query['date'] || $setting.dig(:vertica, :date_column) || 'd'
      end

      def timestamp_column
        parsed_query['timestamp'] || $setting.dig(:vertica, :timestamp_column) || 't'
      end

      def where
        parsed_query['where'] || {}
      end

      def q_db
        @q_db ||= ::Vertica.quote_identifier(db)
      end

      def q_schema
        @q_schema ||= ::Vertica.quote_identifier(schema)
      end

      def q_table
        @q_table ||= ::Vertica.quote_identifier(table)
      end

      def q_date
        @q_date ||= ::Vertica.quote_identifier(date_column)
      end

      def q_timestamp
        @q_timestamp ||= ::Vertica.quote_identifier(timestamp_column)
      end

      # Value specification:
      # * A value looks like an integer string is treated as an integer.
      # * If you want to treat it as as string, surround with double quote or single quote.
      # * A value does not look like an integer is treated as a string.
      # Operator specification:
      # * Only equality is supported now
      def q_where
        @q_where ||= where.map do |col, val|
          begin
            val = Integer(val)
          rescue => e
            if val.start_with?("'") and val.end_with?("'")
              val = val[1..-2]
            elsif val.start_with?('"') and val.end_with?('"')
              val = val[1..-2]
            end
          end
          "#{::Vertica.quote_identifier(col)} = #{::Vertica.quote(val)}"
        end.join(' AND ')
      end
    end
  end
end
