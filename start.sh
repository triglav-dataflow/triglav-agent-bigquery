#!/bin/sh
ABSPATH=$(cd $(dirname $0) && pwd)/$(basename $0)
APP_ROOT=$(dirname $ABSPATH)
if [ -z "${SHARED_ROOT}" ]; then SHARED_ROOT=.; fi

CMD="bundle exec triglav-agent-vertica --dotenv -c config.yml --status ${SHARED_ROOT}/status.yml --token ${SHARED_ROOT}/token.yml"
echo $CMD
$CMD
