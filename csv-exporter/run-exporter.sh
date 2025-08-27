#/bin/bash

export AWS_ACCESS_KEY_ID=$(yc lockbox payload get --id e6qgcdkjdol8skknr0eh --version-id e6qpnkcbh5ochdkjrm7u --key ACCESS_KEY_ID)
export AWS_SECRET_ACCESS_KEY=$(yc lockbox payload get --id e6qgcdkjdol8skknr0eh --version-id e6qpnkcbh5ochdkjrm7u --key SECRET_ACCESS_KEY)
export AWS_DEFAULT_REGION=ru-central1

go run csv-exporter.go \
  --dynamo-docapi-endpoint=https://docapi.serverless.yandexcloud.net/ru-central1/b1gnuj3pj1bhculo16t8/etnr2g6flu3sjlsfgjtl \
  --status=Finished \
  --task-id=doe-200_gan-1-19-19500 \
  --output=doe-200_gan-1-19-19500-final.csv
