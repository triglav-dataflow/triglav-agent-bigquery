require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'

def json_key
  File.read(ARGV[0])
end

def client
  return @cached_client if @cached_client && @cached_client_expiration > Time.now

  client = Google::Apis::BigqueryV2::BigqueryService.new

  scope = "https://www.googleapis.com/auth/bigquery"

  key = StringIO.new(json_key)
  auth = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: scope)
  client.authorization = auth

  @cached_client_expiration = Time.now + 1800
  @cached_client = client
end

def project
  @project ||= JSON.parse(json_key)['project_id']
end

def get_table(dataset:, table:)
  begin
    puts "Get table... #{project}:#{dataset}.#{table}"
    response = client.get_table(project, dataset, table)
  rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
    if e.status_code == 404 # not found
      raise "Table #{project}:#{dataset}.#{table} is not found"
    end

    response = {status_code: e.status_code, message: e.message, error_class: e.class}
    raise Error, "Failed to get_table(#{project}, #{dataset}, #{table}), response:#{response}"
  end

  result = {}
  if response
    result = {
      table_id: response.id,
      creation_time: response.creation_time.to_i, # millisec
      last_modified_time: response.last_modified_time.to_i, # millisec
      location: response.location,
      num_bytes: response.num_bytes.to_i,
      num_rows: response.num_rows.to_i,
    }
  end

  result.merge!({ responses: { get_table: response } })
end

if ARGV.size < 3
  $stderr.puts "get_table <service_account.json> <dataset> <table>"
  exit 1
end

require 'pp'
pp get_table(dataset: ARGV[1], table: ARGV[2])
# {:table_id=>
#   "xxx:yyy.zzz",
#  :creation_time=>1485324316798,
#  :last_modified_time=>1487920243132,
#  :location=>"US",
#  :num_bytes=>15817422,
#  :num_rows=>345736,
#  :responses=>
#   {:get_table=>
#     #<Google::Apis::BigqueryV2::Table:0x007ff58bc99dd8
#      @creation_time="1485324316798",
#      @etag="\"wWvNncJfeAdSHVaIWRpICxBS7AM/MTQ4NTMyNDMxNjc5OA\"",
#      @id="xxx:yyy.zzz",
#      @kind="bigquery#table",
#      @last_modified_time="1487920243132",
#      @location="US",
#      @num_bytes="15817422",
#      @num_long_term_bytes="0",
#      @num_rows="345736",
#      @schema=
#       #<Google::Apis::BigqueryV2::TableSchema:0x007ff58bcc8750
#        @fields=
#         [#<Google::Apis::BigqueryV2::TableFieldSchema:0x007ff58bcbb398
#           @mode="NULLABLE",
#           @name="d",
#           @type="DATE">,
#          #<Google::Apis::BigqueryV2::TableFieldSchema:0x007ff58cc0b1b8
#           @mode="NULLABLE",
#           @name="t",
#           @type="TIMESTAMP">,
#          #<Google::Apis::BigqueryV2::TableFieldSchema:0x007ff58cc08990
#           @mode="NULLABLE",
#           @name="queue",
#           @type="STRING">,
#          #<Google::Apis::BigqueryV2::TableFieldSchema:0x007ff58cbfa750
#           @mode="NULLABLE",
#           @name="latency",
#           @type="FLOAT">,
#          #<Google::Apis::BigqueryV2::TableFieldSchema:0x007ff58cbfb1c8
#           @mode="NULLABLE",
#           @name="size",
#           @type="INTEGER">]>,
#      @self_link=
#       "https://www.googleapis.com/bigquery/v2/projects/xxx/datasets/yyy/tables/zzz",
#      @streaming_buffer=
#       #<Google::Apis::BigqueryV2::Streamingbuffer:0x007ff58d074df8
#        @estimated_bytes="19026",
#        @estimated_rows="504",
#        @oldest_entry_time="1487919600000">,
#      @table_reference=
#       #<Google::Apis::BigqueryV2::TableReference:0x007ff58cc38f28
#        @dataset_id="yyy",
#        @project_id="xxx",
#        @table_id="zzz">,
#      @time_partitioning=
#       #<Google::Apis::BigqueryV2::TimePartitioning:0x007ff58cc29fa0
#        @type="DAY">,
#      @type="TABLE">}}

# Use
# SELECT partition_id,creation_time, last_modified_time from [mydataset.table1$__PARTITIONS_SUMMARY__];
# to get last_modified_time of partition
