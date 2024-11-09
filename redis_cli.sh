#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
#
# Learn more: https://codecrafters.io/program-interface

set -e # Exit early if any commands fail



redis-cli XADD stream_key 0-1 temperature 95
redis-cli XADD other_stream_key 0-2 humidity 97
redis-cli XREAD streams stream_key other_stream_key 0-0 0-1