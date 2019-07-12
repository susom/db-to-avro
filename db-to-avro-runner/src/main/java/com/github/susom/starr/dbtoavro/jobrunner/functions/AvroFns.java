package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Query;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import io.reactivex.Observable;

public interface AvroFns {

  /**
   * Runs a query and saves as Avro
   *
   * @param query query to export as Avro
   * @return saved AvroFile instances
   */
  Observable<AvroFile> saveAsAvro(Query query);

  /**
   * Constructs a query optimized for splitting a very large table into multiple avro files. If the table does not meet
   * splitting criteria, or this database does not have an optimized export, an empty observable is returned.
   *
   * @param table table to split
   * @param targetSize Split the table into partitions of targetSize bytes. Compression may reduce actual file size
   * @param pathPattern filename pattern for query files
   * @return unsaved AvroFile instances
   */
  Observable<Query> optimizedQuery(final Table table, final long targetSize, final String pathPattern);

  /**
   * Constructs a simple query for exporting a table using a single SQL query. (eg. SELECT * FROM ...)
   *
   * @param table table to split
   * @param targetSize Split the table into partitions of targetSize bytes. Compression may reduce actual file size
   * @param pathPattern filename pattern for query files
   * @return unsaved AvroFile instances
   */
  Observable<Query> query(final Table table, final long targetSize, final String pathPattern);

}