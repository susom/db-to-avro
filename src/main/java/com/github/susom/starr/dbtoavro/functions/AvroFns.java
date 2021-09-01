package com.github.susom.starr.dbtoavro.functions;

import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Query;
import io.reactivex.Single;

public interface AvroFns {

  /**
   * Runs a query and saves as Avro
   *
   * @param query query to execute and save as Avro
   * @return saved AvroFile instances
   */
  Single<AvroFile> saveAsAvro(Query query);
}
