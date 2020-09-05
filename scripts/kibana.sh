#!/bin/bash

docker run --link elastic:elasticsearch -p 5601:5601 --name kibana docker.elastic.co/kibana/kibana:7.9.1