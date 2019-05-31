package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import io.reactivex.Observable;

public interface Exporter {

  /**
   * Creates avro files based on job specifications
   *
   * @param job job definition
   * @param loader database loader implementation
   * @return Completable containing result or error
   */
  Observable<AvroFile> run(Job job, Loader loader);

}
