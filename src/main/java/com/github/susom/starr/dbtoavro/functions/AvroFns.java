package com.github.susom.starr.dbtoavro.functions;

import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Table;
import io.reactivex.Single;

public interface AvroFns {

  /**
   * Runs a query and saves as Avro
   *
   * @param table table to save as Avro
   * @return saved AvroFile instances
   */
  Single<AvroFile> saveAsAvro(Table table);

}
