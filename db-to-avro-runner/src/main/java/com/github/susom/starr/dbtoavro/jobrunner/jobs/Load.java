package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import io.reactivex.Single;

public abstract class Load {

  protected final Config config;

  public Load(Config config) {
    this.config = config;
  }

  /**
   * Restores a database backup into a running docker container
   *
   * @param job job definition
   * @param logger job logger
   * @return Completable containing database, or error
   */
  public abstract Single<Warehouse> run(Job job, JobLogger logger);

}
