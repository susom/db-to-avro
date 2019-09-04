package com.github.susom.starr.dbtoavro.jobs;

import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import io.reactivex.Observable;

public interface Exporter {

  /**
   * Creates avro files based on job specifications and a provided database schema
   *
   * @param job job definition
   * @param loader database loader implementation
   * @return Completable containing result or error
   */
  Observable<AvroFile> run(Job job, Loader loader);

}
