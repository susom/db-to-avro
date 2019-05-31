package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import io.reactivex.Single;

public interface Loader {

  /**
   * Loads a database schema
   *
   * @param job job definition
   * @return Completable containing database, or error
   */
  Single<Database> run(Job job);

}
