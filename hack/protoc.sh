#!/bin/bash

PROTOC_ALL_IMAGE=${PROTOC_ALL_IMAGE:-"docker.io/namely/protoc-all:1.47_2"}
PROTO_PATH=pkg/rpc
LANGUAGE=go

proto_modules="remote_cache"

echo "generate protos..."

for module in ${proto_modules}; do
	if docker run --rm -v $PWD:/defs ${PROTOC_ALL_IMAGE} \
		-d ${PROTO_PATH}/$module -i . \
		-l ${LANGUAGE} -o . \
		--go-source-relative ; then
		echo "generate protos ${module} successfully"
	else
		echo "generate protos ${module} failed"
	fi
done
