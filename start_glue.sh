#!/bin/sh

PROFILE=$1

docker run -d --rm --name glue \
	--workdir /home/hadoop/workspace \
	-p 127.0.0.1:8888:8888 -p 127.0.0.1:4040:4040 \
	-v ~/.aws:/home/hadoop/.aws \
	-v $(pwd):/home/hadoop/workspace \
	-e AWS_PROFILE=$PROFILE \
	glue5 
