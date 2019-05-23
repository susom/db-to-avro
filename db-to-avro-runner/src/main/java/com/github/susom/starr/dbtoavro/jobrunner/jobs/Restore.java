package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import io.reactivex.Maybe;
import java.util.List;

public abstract class Restore {

  protected Config config;

  public Restore(Config config) {
    this.config = config;
  }

  /**
   * Restores a database backup into a docker container
   *
   * @param job job definition
   * @param logger job logger
   * @return Completable containing result or error
   */
  abstract public Maybe<Result> run(Job job, JobLogger logger);

  /**
   * Encapsulates results of running the above fn
   */
  static public class Result {

    String containerId;
    List<Table> tables;

    public Result(String containerId, List<Table> tables) {
      this.containerId = containerId;
      this.tables = tables;
    }

  }
}
