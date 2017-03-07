module CreateTable
  def self.included(klass)
    klass.extend(self)
  end

  def project
    JSON.parse(File.read(connection_info[:credentials_file]))['project_id'] rescue ENV['BIGQUERY_PROJECT'] || raise('project id is empty')
  end

  def dataset
    ENV['BIGQUERY_DATASET'] || 'triglav_test'
  end

  def table
    'test_table'
  end

  def partitioned_table
    'test_partitioned_table'
  end

  def with_ignore_already_exists
    begin
      yield
    rescue Google::Apis::ClientError => e
      raise e if e.message !~ /Already Exists/
    end
  end

  def with_ignore_not_found
    begin
      yield
    rescue Google::Apis::ClientError => e
      raise e unless e.message =~ /notFound/
    end
  end

  def setup_tables
    with_ignore_already_exists { create_dataset }
    with_ignore_already_exists { create_table(table: table) }
    with_ignore_already_exists { create_table(table: "#{table}_20170306") }
    with_ignore_already_exists { create_table(table: "#{table}_20170307") }
    with_ignore_already_exists { create_table(table: "#{table}_23_20170306") }
    with_ignore_already_exists { create_table(table: "#{table}_23_20170307") }
    with_ignore_already_exists { create_partitioned_table(table: partitioned_table) }
    with_ignore_already_exists { create_partition(table: "#{partitioned_table}$20170306") }
    with_ignore_already_exists { create_partition(table: "#{partitioned_table}$20170307") }
  end

  def teardown_tables
    with_ignore_not_found { delete_dataset }
  end

  def create_dataset(dataset: self.dataset)
    body = {
      dataset_reference: {
        project_id: project,
        dataset_id: dataset,
      }
    }
    $logger.debug { "insert_dataset(#{project}, #{body}, {})" }
    client.insert_dataset(project, body, {})
  end

  def create_table(dataset: self.dataset, table: self.table, options: {})
    body = {
      table_reference: {
        table_id: table,
      },
      schema: {
        fields: [],
      }
    }
    if options['time_partitioning']
      body[:time_partitioning] = {
        type: options['time_partitioning']['type'],
        expiration_ms: options['time_partitioning']['expiration_ms'],
      }
    end
    $logger.debug { "insert_table(#{project}, #{dataset}, #{body}, {})" }
    client.insert_table(project, dataset, body, {})
  end

  def create_partitioned_table(dataset: self.dataset, table: self.partitioned_table, options: {})
    options['time_partitioning'] = {'type'=>'DAY'}
    create_table(dataset: dataset, table: table, options: options)
  end

  def create_partition(dataset: self.dataset, table: )
    $logger.debug { "create_partition(#{project}, #{dataset}, #{table})" }
    body = {
      job_reference: {
        project_id: project,
        job_id: SecureRandom.uuid,
      },
      configuration: {
        load: {
          destination_table: {
            project_id: project,
            dataset_id: dataset,
            table_id: table,
          },
          schema: {
            fields: [{name:'dummy',type:'STRING',mode:'NULLABLE'}],
          },
          source_format: 'CSV'
        }
      }
    }
    Tempfile.create('_') do |fp|
      fp.puts("dummy")
      fp.flush
      opts = {
        upload_source: fp.path,
        content_type: "application/octet-stream",
      }
      $logger.debug { "insert_job(#{project}, #{body}, #{opts})" }
      response = client.insert_job(project, body, opts)
      response = wait_load(response)
      if response.status.errors
        raise
      end
    end
  end

  def delete_dataset(dataset: self.dataset)
    opts = {delete_contents: true}
    $logger.debug { "delete_dataset(#{project}, #{dataset}, #{opts})" }
    client.delete_dataset(project, dataset, opts)
  end

  def client
    connection.client
  end

  def connection
    return @connection if @connection
    @connection ||= Triglav::Agent::Bigquery::Connection.new(connection_info)
  end

  def connection_info
    @connection_info ||= $setting.dig(:bigquery, :connection_info)[:'https://bigquery.cloud.google.com/table/']
  end

  private

  def wait_load(response)
    started = Time.now

    wait_interval = 3
    max_polling_time = 10
    _response = response

    while true
      job_id = _response.job_reference.job_id
      elapsed = Time.now - started
      status = _response.status.state
      if status == "DONE"
        $logger.debug {
          "job completed... " \
          "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
        }
        break
      elsif elapsed.to_i > max_polling_time
        message = "job checking... " \
          "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[TIMEOUT]"
          $logger.debug { message }
          raise(message)
      else
        $logger.debug {
          "job checking... " \
          "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
        }
        sleep wait_interval
        _response = client.get_job(project, job_id)
      end
    end

    if _errors = _response.status.errors
      msg = "failed during waiting a job, get_job(#{project}, #{job_id}), errors:#{_errors.map(&:to_h)}"
      if _errors.any? {|error| error.reason == 'backendError' }
        raise "BackendError, #{msg}"
      elsif _errors.any? {|error| error.reason == 'internalError' }
        raise "InternalError, #{msg}"
      else
        raise "Error, #{msg}"
      end
    end

    $logger.debug { "job response... job_id:[#{job_id}] response.statistics:#{_response.statistics.to_h}" }

    _response
  end



end
