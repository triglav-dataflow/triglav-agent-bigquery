#!/bin/sh
test -f config.yml || cp example/config.yml config.yml
test -f .env || cp example/example.env .env
