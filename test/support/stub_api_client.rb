module StubApiClient
  def self.included(klass)
    klass.extend(self)
  end

  def stub_api_client
    obj = Object.new
    stub(Triglav::Agent::ApiClient).new { obj }
    stub(obj).list_aggregated_resources { [dummy_resource] }
    stub(obj).send_messages { }
  end

  def dummy_resource
    TriglavClient::AggregatedResourceEachResponse.new(
      uri: 'https://bigquery.cloud.google.com/table/project:triglav_test.test_table',
      unit: 'daily',
      timezone: '+09:00',
      span_in_days: 2,
    )
  end
end
