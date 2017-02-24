require 'triglav/agent/base/connection'
require 'uri'
require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'
require 'securerandom'

# monkey patch not to create representable objects which consumes lots of memory
# @see http://qiita.com/sonots/items/1271f3d426cda6c891c0
module Google
  module Apis
    module BigqueryV2
      class BigqueryService < Google::Apis::Core::BaseService
        def get_job_query_results(project_id, job_id, max_results: nil, page_token: nil, start_index: nil, timeout_ms: nil, fields: nil, quota_user: nil, user_ip: nil, options: nil, &block)
          command =  make_simple_command(:get, 'projects/{projectId}/queries/{jobId}', options)
          # command.response_representation = Google::Apis::BigqueryV2::GetQueryResultsResponse::Representation # monkey patch
          command.response_class = Google::Apis::BigqueryV2::GetQueryResultsResponse
          command.params['projectId'] = project_id unless project_id.nil?
          command.params['jobId'] = job_id unless job_id.nil?
          command.query['maxResults'] = max_results unless max_results.nil?
          command.query['pageToken'] = page_token unless page_token.nil?
          command.query['startIndex'] = start_index unless start_index.nil?
          command.query['timeoutMs'] = timeout_ms unless timeout_ms.nil?
          command.query['fields'] = fields unless fields.nil?
          command.query['quotaUser'] = quota_user unless quota_user.nil?
          command.query['userIp'] = user_ip unless user_ip.nil?
          execute_or_queue_command(command, &block)
        end
      end
    end
  end
end

module Triglav::Agent
  module Bigquery
    class Connection < Base::Connection
      attr_reader :connection_info

      def initialize(connection_info)
        @connection_info = connection_info
      end

      # @return [Hash] {table_id:, creation_time:, last_modified_time:, location:, num_bytes:, num_rows:}
      def get_table(dataset:, table:)
        begin
          logger.debug { "Get table... #{project}:#{dataset}.#{table}" }
          response = client.get_table(project, dataset, table)
        rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
          if e.status_code == 404 # not found
            raise NotFoundError, "Table #{project}:#{dataset}.#{table} is not found"
          end

          response = {status_code: e.status_code, message: e.message, error_class: e.class}
          raise Error, "Failed to get_table(#{project}, #{dataset}, #{table}), response:#{response}"
        end

        result = {
          table_id: response.id,
          creation_time: response.creation_time.to_i, # millisec
          last_modified_time: response.last_modified_time.to_i, # millisec
          location: response.location,
          num_bytes: response.num_bytes.to_i,
          num_rows: response.num_rows.to_i,
        }
      end

      # @return [Array] [partition_id, creation_time, last_modified_time]
      def get_partitions_summary(dataset:, table:)
        query("select partition_id,creation_time,last_modified_time from [#{dataset}.#{table}$__PARTITIONS_SUMMARY__]")
      end

      private

      def query(q, options = {})
        started = Time.now
        current_row = 0

        body  = {
          job_reference: {
            project_id: project_id,
            job_id: "job_#{SecureRandom.uuid}",
          },
          configuration: {
            query: {
              query: q,
              use_legacy_sql: true,
              use_query_cache: true,
            },
            dry_run: options[:dry_run],
          },
        }
        opts = {}

        $logger.info { "insert_job(#{project_id}, #{body}, #{opts})" }
        job_res = connection.insert_job(project_id, body, opts)

        if options[:dry_run]
          {
            totalRows: nil,
            totalBytesProcessed: job_res.statistics.query.total_bytes_processed,
            cacheHit: job_res.statistics.query.cache_hit,
          }
        else
          job_id = job_res.job_reference.job_id

          res = {}
          while true
            res = JSON.parse(connection.get_job_query_results(
              project_id,
              job_id,
            ), symbolize_names: true)
            break if res[:jobComplete]
            sleep 3

            if (Time.now - started).to_i > HARD_TIMEOUT_SEC
              raise RuntimeError.new("Query is timeout")
            end
          end

          if res[:rows]
            res[:rows].each(&block)
            current_row += res[:rows].size
          end
          total_rows = res[:totalRows].to_i

          while current_row < total_rows
            res = JSON.parse(connection.get_job_query_results(
              project_id,
              job_id,
              start_index: current_row
            ), symbolize_names: true)
            if res[:rows]
              res[:rows].each(&block)
              current_row += res[:rows].size
            end
          end

          res
        end
      end

      def connection
        return @cached_client if @cached_client && @cached_client_expiration > Time.now

        client = Google::Apis::BigqueryV2::BigqueryService.new
        client.request_options.retries = retries
        client.request_options.timeout_sec = timeout_sec
        client.request_options.open_timeout_sec = open_timeout_sec

        scope = "https://www.googleapis.com/auth/bigquery"

        key = StringIO.new(config[:json_key])
        auth = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: scope)
        client.authorization = auth

        @cached_client_expiration = Time.now + 1800
        @cached_client = client
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
