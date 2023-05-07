#!/bin/bash

docker build -t striker-bot -f bot/Dockerfile .
docker build -t striker-demoparse -f microservices/demoparse/Dockerfile .
docker build -t striker-login-provisioner -f microservices/login-provisioner/Dockerfile .
docker build -t striker-uploader -f microservices/uploader/Dockerfile .