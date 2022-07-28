#!/bin/bash

SA_PASSWORD=${SA_PASSWORD:-Restore!}
BAKFILE=${BAKFILE:-echo.bak}

echo "Waiting for the SQL Server to come up"
sleep 120s

echo "Copying /bak/$BAKFILE to /var/opt/mssql/backup/restore.bak"
mkdir -p /var/opt/mssql/backup
cp /bak/$BAKFILE /var/opt/mssql/backup/restore.bak

#get the list of databases to restore.
databases=($(/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "${SA_PASSWORD}" -Q "RESTORE FILELISTONLY FROM DISK = '/var/opt/mssql/backup/restore.bak'" | head -n -2 | sed 1,2d | awk '{print $1}'))
dbfiles=($(/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "${SA_PASSWORD}" -Q "RESTORE FILELISTONLY FROM DISK = '/var/opt/mssql/backup/restore.bak'" | head -n -2 | sed 1,2d | awk '{print $4}' | awk 'BEGIN{FS="\\"}{print $NF}'))

echo "Databases from bak file to restore: $databases"

MVSTR=""
for i in ${!databases[@]}; do
    MVSTR=$MVSTR"MOVE '${databases[$i]}' TO '/var/opt/mssql/data/${dbfiles[$i]}',"
done

MVSTR=$(echo $MVSTR | sed 's/,$//')

echo "Moving ... $MVSTR"
/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "${SA_PASSWORD}" -Q "RESTORE DATABASE RestoreDb FROM DISK = '/var/opt/mssql/backup/restore.bak' WITH $MVSTR"

# extract data to avro files
echo "Extracting to avro files"
/usr/work/run-db2avro.sh
