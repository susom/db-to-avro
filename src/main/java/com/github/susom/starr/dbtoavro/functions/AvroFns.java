package com.github.susom.starr.dbtoavro.functions;

import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Table;
import io.reactivex.Single;

public interface AvroFns {

  /**
   * Runs a query and saves as Avro
   *
   * @param query query to export as Avro
   * @return saved AvroFile instances
   */
  Single<AvroFile> saveAsAvro(Query query);

  /**
   * Constructs a simple query for exporting a table using a single SQL query. (eg. SELECT * FROM ...)
   *
   * @param table table to split
   * @param targetSize Split the table into partitions of targetSize bytes. Compression may reduce actual file size
   * @param pathPattern filename pattern for query files
   * @return unsaved AvroFile instances
   */
  Single<Query> query(final Table table, final long targetSize, final String pathPattern);

}
