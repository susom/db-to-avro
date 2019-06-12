## Database Backups to Avro

The goal of this project is to load database backups (eg. Oracle datapump exports, MS Sql Server .bak files) into a
temporary Docker container running the appropriate vendor database, and then export the tables within that restored database as .avro files for loading into other Big data systems. 

### Current Progress

This application is in beta status, and currently supports loading a Microsoft SQL Server backup into a container and exporting as Avro. Oracle support is forthcoming.

### Architecture

On a host running docker, the application will instantiate a database (as a container), load the backup files into the container, and then dump the database as Avro files.
If a database is already running, the restore step can be skipped by providing a JDBC connection string.

The application is asynchronous and multi-threaded using [RxJava2](https://github.com/ReactiveX/RxJava).

### Basic Command Line Usage

```bash
java -jar db-to-avro.jar \
   --flavor=sqlserver \
   --backup-dir=Backups/ \
   --backup-files=AdventureWorksDW2016_EXT.bak
   --catalog=AdventureWorks2016 \
   --schemas=dbo \
   --destination=avro/
```

This will restore the Microsoft AdventureWorks database from a backup set located in `/Backups`, using a single backup file `AdventureWorksDW2016_EXT.bak`.
All tables within schema AdventureWorks2016.dbo will be exported as Avro files to the directory `avro/`

### Exporting an Existing Database

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

This does the same as the above, but instead of loading a database backup, it will use the database pointed at by the connection string.
All tables within schema AdventureWorks2016.dbo will be exported as Avro files to the directory `avro/`

### Command Line Options

```bash
Option (* = required)    Description
---------------------    -----------
* --flavor <Flavor>      database type (sqlserver, oracle)
--connect <String>       jdbc connection string for existing database
--user <String>          database user (existing db)
--password <String>      database password (existing db)
--backup-dir <String>    directory containing backup to restore
--backup-files <String>  comma-delimited list of backup files
--destination <String>   avro destination directory
--post-sql <String>      sql to execute after restore/connect
--pre-sql <String>       sql to execute before restore/connect
* --catalog <String>     catalog to export
--schemas <String>       only export this comma-delimited list of schemas
--tables <String>        only export this comma-delimited list of tables
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
# Attempt to dump a table in parallel using partitions based on PKs
sqlserver.optimized.enable=true

# Oracle defaults
oracle.database.user=system
oracle.database.password=${UUID}
oracle.image=oracle/database:12.2.0.1-ee

# Target size for generated Avro files, based on *uncompressed* source table bytes. Tables under this size will not
# be split into multiple files. Set to zero for unlimited file size.
avro.targetsize=1000000000

# How many rows to fetch per DB query
avro.fetchsize=100000

# Pattern for creating the Avro output.
avro.filename=%{CATALOG}.%{SCHEMA}.%{TABLE}-%{PART}.avro

# Filename for manifest
avro.logfile=job.json

# Avro compression codec (snappy or null recommended)
avro.codec=snappy

# Core-count multiplier determines number of avro threads
avro.core.multiplier=0.75
```

### Future Features

Currently the application consists of a single module "db-to-avro-runner". More modules in the future can be added to call this runner from a REST API, Pubsub queue, etc.

### TODO

* Oracle support

* SUSOM ETL library (from database-goodies) should have options for:
  * Lowercasing entity names
  * Cleaning entity names (remove newlines, "$", etc.)
