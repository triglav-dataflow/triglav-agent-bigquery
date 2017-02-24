require_relative '../helper'
require_relative '../support/create_table'

# This script requires a real connection to bigquery, now
# Configure .env to set proper connection_info of test/support/config.yml
#
# TRIGLAV_URL=http://localhost:7800
# TRIGLAV_USERNAME=triglav_test
# TRIGLAV_PASSWORD=triglav_test
# GOOGLE_APPLICATION_CREDENTIALS: "~/.config/gcloud/application_default_credentials.json"
# BIGQUERY_PROJECT: xxx-xxx-xxx
# BIGQUERY_DATASET: triglav_test
#
# This creates some tables in `triglav_test` (default) dataset
include CreateTable
setup_tables
