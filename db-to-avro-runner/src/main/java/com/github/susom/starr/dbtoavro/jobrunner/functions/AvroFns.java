package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Query;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import io.reactivex.Observable;

public interface AvroFns {

  /**
   * Saves an AvroFile instance to disk
   *
   * @param query SQL query to pass to Avro export
   * @param pathPattern a pattern for naming the Avro output. Supports %{CATALOG}, %{SCHEMA}, %{TABLE}, %{PART}
   * @return saved AvroFile instances
   */
  Observable<AvroFile> saveAvroFile(Query query, String pathPattern);

  /**
   * Constructs a query optimized for splitting a very large table into multiple avro files. If the table does not meet
   * splitting criteria, or this database does not have an optimized export, an empty observable is returned.
   *
   * @param table table to split
   * @param targetSize Split the table into partitions of targetSize bytes. Compression may reduce actual file size
   * considerably.
   * @return unsaved AvroFile instances
   */
  Observable<Query> optimizedQuery(final Table table, final long targetSize);

  /**
   * Constructs a simple query for exporting a table using a single SQL query. (eg. SELECT * FROM ...)
   *
   * @param table table to split
   * @param targetSize Split the table into partitions of targetSize bytes. Compression may reduce actual file size
   * considerably.
   * @return unsaved AvroFile instances
   */
  Observable<Query> query(final Table table, final long targetSize);

}