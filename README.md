# Triglav::Agent::Bigquery

Triglav Agent for BigQuery

## Requirements

* Ruby >= 2.3.0

## Prerequisites

* Bigquery table must have a DATE column for `daily` resource monitor
* Bigquery table must have a TIMESTAMP or TIMESTAMPTZ column for `hourly` resource monitor
* Bigquery view is not supported (since `epoch` column can not be retrieved)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'triglav-agent-bigquery'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install triglav-agent-bigquery

## CLI

```
Usage: triglav-agent-bigquery [options]
    -c, --config VALUE               Config file (default: config.yml)
    -s, --status VALUE               Status stroage file (default: status.yml)
    -t, --token VALUE                Triglav access token storage file (default: token.yml)
        --dotenv                     Load environment variables from .env file (default: false)
    -h, --help                       help
        --log VALUE                  Log path (default: STDOUT)
        --log-level VALUE            Log level (default: info)
```

Run as:

```
TRIGLAV_ENV=development bundle exec triglav-agent-bigquery --dotenv -c config.yml
```

## Configuration

Prepare config.yml as [example/config.yml](./example/config.yml).

You can use erb template. You may load environment variables from .env file with `--dotenv` option as an [example/example.env](./example/example.env) file shows.

### serverengine section

You can specify any [serverengine](https://github.com/fluent/serverengine) options at this section

### triglav section

Specify triglav api url, and a credential to authenticate.

The access token obtained is stored into a token storage file (--token option).

### bigquery section

This section is the special section for triglav-agent-bigquery.

* **monitor_interval**: The interval to watch tables (number, default: 60)
* **connection_info**: key-value pairs of bigquery connection info where keys are resource URI pattern in regular expression, and values are connection infomation

### Specification of Resource URI

Resource URI must be a form of:

```
https://bigquery.cloud.google.com/table/#{project}:#{dataset}.#{table}
```

`#{table}` also accepts strftime formatted suffix such as

```
#{table}_%Y%m%d
```

and strftime formatted partition decorator for a partitioned table such as

```
#{table}$%Y%m%d
```

## How it behaves

1. Authenticate with triglav
  * Store the access token into the token storage file
  * Read the token from the token storage file next time
  * Refresh the access token if it is expired
2. Repeat followings in `monitor_interval` seconds:
3. Obtain resource (table) lists of the specified prefix (keys of connection_info) from triglav.
4. Connect to bigquery with an appropriate connection info for a resource uri, and find tables which are newer than last check.
5. Store checking information into the status storage file for the next time check.

## Development

### Prepare

```
./prepare.sh
```

Edit `.env` or `config.yml` file directly.

### Start

Start up triglav api on localhost.

Run triglav-anget-bigquery as:

```
TRIGLAV_ENV=development bundle exec triglav-agent-bigquery --dotenv --debug -c example/config.yml
```

The debug mode with --debug option ignores the `last_epoch` value in status file.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/triglav-workflow/triglav-agent-bigquery. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

