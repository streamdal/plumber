#!/bin/bash
set -e
if [ -f "/ca/ca.crt" ]; then
  ln -s /ca/ca.crt /usr/local/share/ca-certificates/ca.crt
  update-ca-certificates
fi 
if [ -f "/vault/secrets/global" ]; then
  for config in `ls -d /vault/secrets/*`; do
     source $config
  done
fi
exec "$@"
