#!/usr/bin/env bash
# 
# Thank you Suru Dissanaike!
# https://medium.com/himinds/mqtt-broker-with-secure-tls-and-docker-compose-708a6f483c92

IP="192.168.1.22"
SUBJECT_CA="/C=SE/ST=Stockholm/L=Stockholm/O=himinds/OU=CA/CN=$IP"
SUBJECT_SERVER="/C=SE/ST=Stockholm/L=Stockholm/O=himinds/OU=Server/CN=$IP"
SUBJECT_CLIENT="/C=SE/ST=Stockholm/L=Stockholm/O=himinds/OU=Client/CN=$IP"

function generate_CA () {
   echo "$SUBJECT_CA"
   openssl req -x509 -nodes -sha256 -newkey rsa:2048 -subj "$SUBJECT_CA"  -days 365 -keyout ./ssl/ca.key -out ./ssl/ca.crt
}

function generate_server () {
   echo "$SUBJECT_SERVER"
   openssl req -nodes -sha256 -new -subj "$SUBJECT_SERVER" -keyout ./ssl/server.key -out ./ssl/server.csr
   openssl x509 -req -sha256 -in ./ssl/server.csr -CA ./ssl/ca.crt -CAkey ./ssl/ca.key -CAcreateserial -out ./ssl/server.crt -days 365
}

function generate_client () {
   echo "$SUBJECT_CLIENT"
   openssl req -new -nodes -sha256 -subj "$SUBJECT_CLIENT" -out ./ssl/client.csr -keyout ./ssl/client.key 
   openssl x509 -req -sha256 -in ./ssl/client.csr -CA ./ssl/ca.crt -CAkey ./ssl/ca.key -CAcreateserial -out ./ssl/client.crt -days 365
}

function copy_keys_to_broker () {
   mkdir -p ./mosquitto/certs
   sudo cp ./ssl/ca.crt ./mosquitto/certs/
   sudo cp ./ssl/server.crt ./mosquitto/certs/
   sudo cp ./ssl/server.key ./mosquitto/certs/
}

mkdir -p ./ssl

generate_CA
generate_server
generate_client
copy_keys_to_broker

