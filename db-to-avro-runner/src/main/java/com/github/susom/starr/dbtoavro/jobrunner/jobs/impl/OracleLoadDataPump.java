package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.OracleDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.OracleDockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import com.github.susom.starr.dbtoavro.jobrunner.util.RetryWithDelay;
import io.reactivex.Single;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleLoadDataPump implements Loader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerLoadBackup.class);

  private OracleDockerFns docker;
  private OracleDatabaseFns db;
  private Config config;

  public OracleLoadDataPump(Config config, DatabaseProviderRx.Builder dbb) {
    this.config = config;
    this.db = new OracleDatabaseFns(config, dbb);
  }

  @Override
  public Single<Database> run(Job job) {

    LOGGER.info("Starting Oracle data pump restore");

    List<String> mounts = new ArrayList<>();
    mounts.add(new File(job.backupDir) + ":/backup");
    if (config.getString("oracle.mounts") != null) {
      mounts.addAll(Arrays.asList(config.getStringOrThrow("oracle.mounts").split("\\s*,\\s*")));
    }

    List<String> ports = Arrays.asList(config.getString("oracle.ports", "1521:1521").split("\\s*,\\s*"));

    docker = new OracleDockerFns(config);

    return docker.create(mounts, ports).flatMap(containerId ->
        docker.start(containerId)
            .doOnComplete(() -> LOGGER
                .info(String.format(Locale.CANADA, "Container %s started, waiting for database to boot", containerId)))
            .andThen(docker.healthCheck(containerId).retryWhen(new RetryWithDelay(180, 5000)))
            .andThen(docker.execSqlShell(containerId, job.preSql))
            .doOnNext(line -> LOGGER.info(line.getData()))
            .doOnComplete(() -> LOGGER.info("Database pre-sql completed"))
            .doOnComplete(() -> LOGGER.info("Starting database restore"))
            .ignoreElements()
            .andThen(docker.impdp(containerId, job.backupFiles))
            .doOnNext(line -> LOGGER.info(line.getData()))
            .ignoreElements()
            .doOnComplete(() -> LOGGER.info("Restore completed"))
            .andThen(docker.execSqlShell(containerId, job.postSql)
                .doOnNext(line -> LOGGER.info(line.getData())))
            .ignoreElements()
            .doOnComplete(() -> LOGGER.info("Database post-sql completed"))
            .andThen(db.getDatabase(containerId)
                .doFinally(() -> LOGGER.info("Database introspection complete"))
            )
    );

  }

}
