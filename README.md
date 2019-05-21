## Database Backups to Avro

The goal of this project is to load database backups (eg. Oracle datapump exports, MS Sql Server .bak files) into a
temporary Docker container running the appropriate vendor database, and then export the tables within that restored database as .avro files for loading into other Big data systems. 

### Current Progress

This application is in alpha status, and currently supports loading a Microsoft SQL Server backup into a container. 

### Planned Features

The application will eventually support:
* Loading Oracle datapump exports and Microsoft SQL Server backups into a docker container.

* Selectively exporting schemas / tables within the loaded database as .avro files using various methods depending on the size of the source database: 
  
  * Multi-threaded (table-level) Avro creation using [Stride Util](https://github.com/susom/stride-util/blob/master/src/main/java/com/github/susom/stride/server/container/OracleEtl.java).
  
  * Multi-threaded Avro (table-level) creation using [Apache Sqoop](https://sqoop.apache.org/) v1 in standalone mode. 
  
  * Multi-threaded (row-level) exporting to Avro using [Apache Sqoop](https://sqoop.apache.org/) v1 on a Hadoop cluster running in Google Cloud Dataproc.   

  * *(possible)* Multi-threaded (row-level) exporting to Avro using [Apache Sqoop](https://sqoop.apache.org/) on a Hadoop cluster running in Docker / Kubernetes. 

### Architecture

Jobs are defined as an asynchronous functional expression using [RxJava2](https://github.com/ReactiveX/RxJava). 
Different types of jobs can be chained together for more complex operations.

Example: 
```java
public static Completable Restore(DockerFns docker, DatabaseFns sql, Job job, JobLogger logger) {

    return docker.create().flatMapCompletable(containerId ->
        docker.start(containerId) // (1)
            .doOnComplete(() -> logger.log(String.format("Container %s started, waiting for database to boot", containerId)))
            .andThen(docker.healthCheck(containerId).retryWhen(new RetryWithDelay(3, 2000))) // (2)

            // Execute pre-sql commands
            .andThen(sql.transact(job.getPreSql())) // (3)
            .doOnComplete(() -> logger.log("Database pre-sql completed"))

            .doOnComplete(() -> logger.log("Starting database restore"))
            .andThen(sql.getRestoreSql(job.getDatabaseName(), job.getBackupFiles()) // (4)
                .flatMapObservable(ddl ->
                    docker.execSqlShell(containerId, ddl) // (5)
                        .observeOn(Schedulers.io())))
            .filter(p -> p.getPercent() != -1)
            .doOnNext(p -> logger.progress(p.getPercent(), 100)).ignoreElements() // (6)
            .doOnComplete(() -> logger.log("Restore completed"))

            .andThen(sql.transact(job.getPostSql())) // (7)
            .doOnComplete(() -> logger.log("Database post-sql completed"))

            .doOnComplete(() -> logger.log("Introspecting schema")) // (8)
            .andThen(sql.getTables(job.getDatabaseName()))
            .doOnSuccess(job::setTables)
            .ignoreElement()

            .doOnComplete(() -> logger.log(String.format("Stopping container %s", containerId))) // (9)
            .andThen(docker.stop(containerId))

            .doOnComplete(() -> logger.log(String.format("Deleting container %s", containerId)))
            .andThen(docker.destroy(containerId))

            .doOnComplete(() -> logger.log("Job complete!")));
  }
```

The example above does the following: 
1. Creates a database docker container and starts it.
2. Waits for the database to go online. 
3. Executes an SQL script to prepare the database for the restore. 
4. Introspects the backup file and creates SQL code for initiating the restore.
5. Initiates the database restore in a new thread.
6. Reports progress to the runner while the database is being restored.
7. Executes an SQL script to clean up.
8. Enumerates the tables with their row counts, stores the result in the Job.
9. Stops and deletes the container. 

During the entire operation, progress of the Job is recorded along with the percent complete during the database restore operation.

After the Job Runner finishes the above operation, the final result is marked as Success or Failed, and the Job is output as JSON for downstream systems. 

### Future Features

Currently the application consists of a single module "jobrunner". More modules in the future can be added to call jobrunner from a REST API, Pubsub queue, etc.

### TODO

Lots, this application is currently under development.
