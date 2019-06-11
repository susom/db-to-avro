package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import io.reactivex.Observable;

public interface AvroFns {

  /**
   * Saves an AvroFile instance to disk
   *
   * @param avroFile avro file to save
   * @return saved AvroFile instances
   */
  Observable<AvroFile> saveAvroFile(AvroFile avroFile);

  /**
   * Emits unsaved AvroFile instances representing subsets of a single table, or nothing if the table can't be partitioned.
   *
   * @param table table to split
   * @param path expression for partitioned file
   * @param targetSize target size of the partition, in (table) bytes. Compression may reduce this size considerably. Specify
   * 0 for no partitioning.
   * @return unsaved AvroFile instances
   */
  Observable<AvroFile> multipleQuery(final Table table, final String path, final long targetSize);

  /**
   * Emits unsaved AvroFile instances created by dumping the entire table using a single SQL query
   *
   * @param table table to split
   * @param path expression for partitioned file
   * @param targetSize target size of the partition, in (table) bytes. Compression may reduce this size considerably. Specify
   * 0 for no partitioning.
   * @return unsaved AvroFile instances
   */
  Observable<AvroFile> singleQuery(final Table table, final String path, final long targetSize);

}