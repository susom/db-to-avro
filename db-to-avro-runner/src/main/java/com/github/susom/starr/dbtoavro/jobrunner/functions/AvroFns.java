package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Partition;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import io.reactivex.Observable;

public interface AvroFns {

  /**
   * Save an entire table without row-level splitting. May emit multiple avro files.
   *
   * @param table to export
   * @param path expression for target file
   * @param targetSize target for number of table-bytes to store in each Avro file
   * @return AvroFile instance(s)
   */
  Observable<AvroFile> saveTableAsAvro(Table table, String path, long targetSize);

  /**
   * Save a subset of a table as an Avro file. May emit multiple avro files.
   *
   * @param partition to save
   * @return AvroFile instance
   */
  Observable<AvroFile> savePartitionAsAvro(Partition partition);

  /**
   * Emits partition instances representing subsets of a single table, or nothing if the table can't be split.
   *
   * @param table table to split
   * @param targetSize target size of avro file
   * @return table segment
   */
  Observable<Partition> getPartitions(Table table, long targetSize);

}