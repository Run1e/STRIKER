#!/bin/bash

docker build -t striker-bot -f bot/Dockerfile .
docker build -t striker-demoparse -f microservices/demoparse/Dockerfile .
docker build -t striker-login-provisioner -f microservices/login-provisioner/Dockerfile .
docker build -t striker-matchinfo -f microservices/matchinfo/Dockerfile .
docker build -t striker-uploader -f microservices/uploader/Dockerfile .
docker build -t striker-archive -f microservices/archive/Dockerfile .