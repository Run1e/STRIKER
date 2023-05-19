#!/bin/bash

docker build -t striker-bot -f bot/Dockerfile .
docker build -t striker-demoparse -f microservices/demoparse/Dockerfile .
docker build -t striker-gateway -f microservices/gateway/Dockerfile .
docker build -t striker-uploader -f microservices/uploader/Dockerfile .