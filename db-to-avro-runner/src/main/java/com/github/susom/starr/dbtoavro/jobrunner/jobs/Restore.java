package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import io.reactivex.Single;

public abstract class Restore {

  protected final Config config;

  public Restore(Config config) {
    this.config = config;
  }

  /**
   * Restores a database backup into a running docker container
   *
   * @param job job definition
   * @param logger job logger
   * @return Completable containing database, or error
   */
  public abstract Single<Database> run(Job job, JobLogger logger);

}
