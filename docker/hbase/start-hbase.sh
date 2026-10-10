#!/bin/bash
set -euo pipefail

start-hbase.sh
exec hbase rest start -p 8080
