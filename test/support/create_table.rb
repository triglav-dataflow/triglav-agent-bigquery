module CreateTable
  def self.included(klass)
    klass.extend(self)
  end

  def host
    connection_info[:host]
  end

  def port
    connection_info[:port]
  end

  def db
    'vdb'
  end

  def schema
    'sandbox'
  end

  def table
    'triglv_test'
  end

  def data
    now = Time.now
    50.times.map do |i|
      t = now - i * 3600
      {
        d: t.strftime("%Y-%m-%d"),
        t: t.strftime("%Y-%m-%d %H:%M:%S"),
        id: i,
        uuid: i.to_s,
      }
    end
  end

  def create_table
    connection.query(<<~SQL)
      CREATE TABLE IF NOT EXISTS #{db}.#{schema}.#{table} (
        d date,
        t timestamp,
        id integer,
        uuid varchar(12)
      );
    SQL
  end

  def insert_data
    data.each do |row|
      connection.query(%Q[
        INSERT INTO #{db}.#{schema}.#{table} VALUES
        ('#{row[:d]}', '#{row[:t]}', #{row[:id]}, '#{row[:uuid]}')
      ])
    end
    connection.query('commit')
  end

  def drop_table
    connection.query("DROP TABLE IF EXISTS #{db}.#{schema}.#{table}")
  end

  def connection
    return @connection if @connection
    @connection ||= Triglav::Agent::Vertica::Connection.new(connection_info)
  end

  def connection_info
    @connection_info ||= $setting.dig(:vertica, :connection_info)[:'vertica://']
  end
end
