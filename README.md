## Database Backups to Avro

The goal of this project is to load database backups (eg. Oracle datapump exports, MS Sql Server .bak files) into a
temporary Docker container running the appropriate vendor database, and then export the tables within that restored database as .avro files for loading into other Big data systems. 

### Current Progress

MSSQL Server backups and Oracle Datapump exports are currently supported. Oracle supports row-level multi-threading for high performance.

### Architecture

On a host running docker, the application will instantiate a database (as a container), load the backup files into the container, and then dump the database as Avro files.
If a database is already running, the restore step can be skipped by providing a JDBC connection string.

The application is asynchronous and multi-threaded using [RxJava2](https://github.com/ReactiveX/RxJava).

### Basic Command Line Usage

MSSQL Server
```bash
java -jar db-to-avro.jar \
   --flavor=sqlserver \
   --backup-dir=backups/ \
   --backup-files=AdventureWorksDW2016_EXT.bak
   --catalog=AdventureWorks2016 \
   --schemas=dbo \
   --destination=avro/
```
This will restore the Microsoft AdventureWorks database from a backup set located in `backups/`, using a single backup file `AdventureWorksDW2016_EXT.bak`.
All tables within schema AdventureWorks2016.dbo will be exported as Avro files to the directory `avro/`

Oracle
```bash
java -jar db-to-avro.jar \
   --flavor=oracle \
   --backup-dir=backups/ \
   --backup-files=HR.par
   --schemas=HRDEMO \
   --destination=avro/
```
This will restore the Oracle HR demo database from a backup set located in `backups/`, using a par file `HR.par`.
All tables within schema HR will be exported as Avro files to the directory `avro/`


### Exporting an Existing Database

MSSQL Server
```bash
java -jar db-to-avro.jar \
  --connect="jdbc:sqlserver://localhost" \
  --user="SA" \
  --password="..."  \
  --flavor=sqlserver  \
  --catalog=AdventureWorks2016 \
  --schemas=dbo  \
  --destination=avro/
```

Oracle
```bash
java -jar db-to-avro.jar \
  --connect="jdbc:oracle:thin:@localhost:1521/ORCLPDB1" \
  --user="system" \
  --password="..."  \
  --flavor=oracle  \
  --schemas=HR  \
  --destination=avro/
```

These do the same as the above, but instead of loading a database backup, it will use an existing database via the connection string.



### Command Line Options

```bash
Option (* = required)    Description
---------------------    -----------
--backup-dir <String>    directory containing backup to restore
--backup-files <String>  comma-delimited list of backup files, or a single .par file
--catalog <String>       catalog to export (Oracle N/A)
--connect <String>       jdbc connection string for existing database
--destination <String>   avro destination directory
--exclude <String>       exclusions in form schema(.table)(.column)
* --flavor <Flavor>      database type (sqlserver, oracle)
--password <String>      database password (existing db)
--post-sql <File>        path of sql file to execute after restore/connect
--pre-sql <File>         path of sql file to execute before restore/connect
--schemas <String>       only export this comma-delimited list of schemas
--tables <String>        only export this comma-delimited list of tables
--user <String>          database user (existing db)
-h, --help               show help
```

### Example Configuration Properties

```java
# Application defaults, can be overridden by a job
#
# Note ${UUID} is substituted everywhere with the same random UUID at startup time

# Docker host for managing containers
docker.host=unix:///var/run/docker.sock

# Make sure this is at least # threads or you will get Hikari connection timeouts
database.pool.size=64

# MS SQL Server defaults
sqlserver.database.url=jdbc:sqlserver://localhost;user=SA;password=${UUID};database=master;autoCommit=false
sqlserver.database.user=SA
sqlserver.database.password=${UUID}
sqlserver.image=mcr.microsoft.com/mssql/server:2017-latest
sqlserver.env=ACCEPT_EULA=Y,SA_PASSWORD=${UUID}
# Attempt to dump a table in parallel using partitions based on PKs (beta)
sqlserver.optimized.enable=false

# Oracle defaults
oracle.database.url=jdbc:oracle:thin:@10.10.10.100:1521/ORCLPDB1
oracle.database.user=system
oracle.database.password=${UUID}
oracle.ports=1521:1521
oracle.image=gcr.io/som-rit-phi-starr-dev/oracle-database:12.2.0.1-ee
oracle.mounts=/mnt/database:/opt/oracle/oradata,/mnt/backups:/backup
oracle.env=ORACLE_CHARACTERSET=WE8ISO8859P1,ORACLE_PWD=${UUID}
oracle.impdp.core.multiplier=1
oracle.optimized.enable=true

# Target size for generated Avro files, based on *uncompressed* source table bytes.
# Set to zero for unlimited file size.
avro.filename=%{SCHEMA}.%{TABLE}-%{PART}.avro
avro.logfile=job.json
avro.codec=snappy
avro.targetsize=1000000000
avro.fetchsize=5000
# Normalize table names (columns always normalized)
avro.tidy=true

# Core-count multiplier determines number of avro threads
avro.core.multiplier=0.75
```

### Future Features

Currently the application consists of a single module "db-to-avro-runner". More modules in the future can be added to call this runner from a REST API, Pubsub queue, etc.

### TODO

* Ability to split based on number of rows, not just table bytes
* Option to save directly to a GCS bucket
* Support for regex in schema/table/column exclusion filters
* db-goodies ETL needs to return number of rows written
  * Validation: Number of rows in database should match number of rows written by db-goodies ETL
* Resume feature: cancelled/crashed jobs should resume at last table exported
* Automation
  - Ability to self-bootstrap into a new VM created in GCP and monitor output (?)
  - Job runner that reads VM metadata for job input (?)
  - Pub/sub job runner (?)
*
* Unit tests(!)

### Known Issues
* Table and column names are normalized in db-goodies ETL, which is not returned in job output log.
* SQL server is single-threaded (per-table) and is much slower than Oracle
* Excluded tables are not explicitly noted in job output (they just aren't listed)
