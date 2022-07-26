#!/bin/sh
#this file should be run as sudo

LOCAL_SRC_DIR=/home/eloh/bak
LOCAL_DEST_DIR=/home/eloh/avro_echo
BAKFILE=echo_20220718.bak

#this is the password used for the docker mssql instance
SA_PASSWORD=Rest0re!

has_image=`docker image ls | grep bak2avro`
if [ -z "$has_image"]; then
	docker build -t mssqlbak2avro .
fi

cp ../../../target/db-to-avro-2.0.jar .

docker run --rm -d \
	-e "ACCEPT_EULA=Y" -e "SA_PASSWORD=$SA_PASSWORD" \
	-e BAKFILE="$BAKFILE" \
	--name mssqlbak_avro \
	-v ${LOCAL_SRC_DIR}:/bak \
	-v ${LOCAL_DEST_DIR}:/avro \
	-p 1433:1433 mssqlbak2avro
