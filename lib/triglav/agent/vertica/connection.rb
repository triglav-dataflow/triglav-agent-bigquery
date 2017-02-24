require 'triglav/agent/base/connection'
require 'vertica'
require 'uri'

module Triglav::Agent
  module Vertica
    class Connection < Base::Connection
      attr_reader :connection_info

      def initialize(connection_info)
        @connection_info = connection_info
      end

      def query(sql)
        connection.query(sql)
      end

      private

      def connection
        return @connection if @connection
        connection_info = @connection_info.dup
        connection_info.delete(:resource_pool)
        connection_info.delete(:memorycap)
        begin
          @connection = ::Vertica.connect(connection_info)
        rescue => e
          $logger.error { "Failed to connect #{connection_info[:host]}:#{connection_info[:port]} with #{connection_info[:user]}" }
          raise e
        end
        $logger.info { "Connected to #{connection_info[:host]}:#{connection_info[:port]}" }
        set_resource_pool
        set_memorycap
        @connection
      end

      def set_resource_pool
        if @connection_info[:resource_pool] and !@connection_info[:resource_pool].empty?
          @connection.query("set session resource_pool = '#{@connection_info[:resource_pool]}'")
        end
      end

      def set_memorycap
        if @connection_info[:memorycap] and !@connection_info[:memorycap].empty?
          @connection.query("set session memorycap = '#{@connection_info[:memorycap]}'")
        end
      end
    end
  end
end
