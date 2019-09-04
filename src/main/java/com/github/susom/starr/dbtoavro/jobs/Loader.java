package com.github.susom.starr.dbtoavro.jobs;

import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Database;
import io.reactivex.Completable;
import io.reactivex.Single;

public interface Loader {

  /**
   * Loads a database schema
   *
   * @param job job definition
   * @return Single containing database, or error
   */
  Single<Database> run(Job job);

  /**
   * Cleans up after using loader
   *
   * @return Completable, or error
   */
  Completable stop();

}
