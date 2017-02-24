require 'triglav/agent/base/connection'
require 'bigquery'
require 'uri'

module Triglav::Agent
  module Bigquery
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
          @connection = ::Bigquery.connect(connection_info)
        rescue => e
          $logger.error { "Failed to connect #{connection_info[:host]}:#{connection_info[:port]} with #{connection_info[:user]}" }
          raise e
        end
        $logger.info { "Connected to #{connection_info[:host]}:#{connection_info[:port]}" }
        set_resource_pool
        set_memorycap
        @connection
      end

      # compute_engine, authorized_user, service_account
      def auth_method
        @auth_method ||= ENV['AUTH_METHOD'] || @connection_info.fetch(:auth_method, nil) || credentials['type'] || 'compute_engine'
      end

      def credentials
        JSON.parse(@connection_info.fetch(:credentials, nil) || File.read(credentials_file))
      end

      def credentials_file
        @credentials_file ||= File.expand_path(
          # ref. https://developers.google.com/identity/protocols/application-default-credentials
          ENV['GOOGLE_APPLICATION_CREDENTIALS'] ||
          @connection_info.fetch(:credentials_file, nil) ||
          (File.exist?(global_application_default_credentials_file) ? global_application_default_credentials_file : application_default_credentials_file)
        )
      end

      def application_default_credentials_file
        @application_default_credentials_file ||= File.expand_path("~/.config/gcloud/application_default_credentials.json")
      end

      def global_application_default_credentials_file
        @global_application_default_credentials_file ||= '/etc/google/auth/application_default_credentials.json'
      end

      def config_default_file
        File.expand_path('~/.config/gcloud/configurations/config_default')
      end

      def config_default
        # {'core'=>{'account'=>'xxx','project'=>'xxx'},'compute'=>{'zone'=>'xxx}}
        @config_default ||= File.readable?(config_default_file) ? IniFile.load(config_default_file).to_h : {}
      end

      def service_account_default
        (config_default['core'] || {})['account']
      end

      def project_default
        (config_default['core'] || {})['project']
      end

      def zone_default
        (config_default['compute'] || {})['zone']
      end

      def project
        @project ||= ENV['GOOGLE_PROJECT'] || @connection_info.fetch(:project, nil) || credentials['project_id']
        @project ||= credentials['client_email'].chomp('.iam.gserviceaccount.com').split('@').last if credentials['client_email']
        @project ||= project_default
      end

      def service_account
        @service_account ||= ENV['GOOGLE_SERVICE_ACCOUNT'] || @connection_info.fetch(:service_account, nil) || credentials['client_email'] || service_account_default
      end

      def retries
        @retries ||= ENV['RETRIES'] || @connection_info.fetch(:retries, 5)
      end

      def timeout_sec
        @timeout_sec ||= ENV['TIMEOUT_SEC'] || @connection_info.fetch(:timeout_sec, 300)
      end

      def open_timeout_sec
        @open_timeout_sec ||= ENV['OPEN_TIMEOUT_SEC'] || @connection_info.fetch(:open_timeout_sec, 300)
      end
    end
  end
end
