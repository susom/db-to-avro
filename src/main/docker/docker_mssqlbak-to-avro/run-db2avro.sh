#!/bin/bash
#this file should be run as sudo
SA_PASSWORD=${SA_PASSWORD:-Rest0re!}

#exclude-table option is not working
java -jar /usr/work/db-to-avro.jar --connect="jdbc:sqlserver://localhost:1433;user=SA;password=${SA_PASSWORD};database=master;autoCommit=false" --user="SA" --password="${SA_PASSWORD}" --flavor=sqlserver --catalog=RestoreDb --schema=dbo --destination=/avro --exclude-table=sysdiag --log-file=/avro/${BAKFILE}_av.log
