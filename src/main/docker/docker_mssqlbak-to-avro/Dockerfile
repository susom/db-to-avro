FROM mcr.microsoft.com/mssql/server:2019-latest
USER root

#install java 8
RUN apt-get update
RUN apt-get -y install openjdk-8-jre

# Create work directory
RUN mkdir -p /usr/work
WORKDIR /usr/work

# Copy entrypoint.sh, mssql-restore.sh scripts into working directory
COPY entrypoint.sh /usr/work/
COPY run_mssqlrestore.sh /usr/work/
#bak file should be mounted on a volume
#COPY $BAKFILE /usr/work/

# Copy files used for db-to-avro
COPY db-to-avro-2.0.jar /usr/work/db-to-avro.jar
COPY local.properties /usr/work
COPY run-db2avro.sh /usr/work/

# Grant permissions for the scripts to be executable
RUN chmod u+x mssql-restore.sh
RUN chmod u+x run-db2avro.sh

EXPOSE 1433:1433
CMD /bin/bash ./entrypoint.sh

