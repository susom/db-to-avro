package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.OracleDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.OracleDockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import com.github.susom.starr.dbtoavro.jobrunner.util.RepeatWithDelay;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleLoadDataPump implements Loader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerLoadBackup.class);

  private OracleDockerFns docker;
  private OracleDatabaseFns db;
  private Config config;
  private String containerId;

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

    return docker.create(mounts, ports).flatMap(containerId -> {
          this.containerId = containerId;
          return docker.start(containerId)
              .andThen(docker.isRunning(containerId)
                  .repeatWhen(new RepeatWithDelay(12, 5000))
                  .takeWhile(b -> b.equals(false)).ignoreElements()
              )
              .andThen(docker.isHealthy(containerId)
                  .repeatWhen(new RepeatWithDelay(60, 5000))
                  .takeWhile(b -> b.equals(false)).ignoreElements()
              )
              .andThen(db.isValid()
                  .repeatWhen(new RepeatWithDelay(24, 5000))
                  .takeWhile(b -> b.equals(false)).ignoreElements()
              )
              .andThen(docker.execSqlFile(containerId, job.preSql)
                  .doOnNext(line -> LOGGER.info(line.getData()))
                  .ignoreElements() // TODO: record output
              )
              .andThen(docker.impdp(containerId, job.backupFiles)
                  .doOnNext(line -> LOGGER.info(line.getData()))
                  .ignoreElements() // TODO: record output
              )
              .andThen(docker.execSqlFile(containerId, job.postSql)
                  .doOnNext(line -> LOGGER.info(line.getData()))
                  .ignoreElements() // TODO: record output
              )
              .andThen(db.getDatabase(containerId));
        }
    );

  }

  @Override
  public Completable stop() {
    return new OracleDockerFns(config).stop(this.containerId);
  }

}
