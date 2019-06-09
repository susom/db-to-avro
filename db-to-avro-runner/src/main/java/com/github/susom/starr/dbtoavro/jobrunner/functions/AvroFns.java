package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import io.reactivex.Observable;

public interface AvroFns {

  /**
   * Save an avrofile object to disk
   *
   * @param avroFile avro file to save
   * @return saved AvroFile instance
   */
  Observable<AvroFile> saveAvroFile(AvroFile avroFile);

  /**
   * Emits partitions representing subsets of a single table, or nothing if the table can't be partitioned.
   *
   * @param table table to split
   * @param path expression for partitioned file
   * @param size target size of the partition, in (table) bytes. Compression may reduce this size considerably. Specify
   * 0 for no partitioning.
   * @return table segment
   */
  Observable<AvroFile> getPartitions(Table table, String path, long size);

}