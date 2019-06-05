package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.BoundedRange;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database.Catalog.Schema.Table;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface AvroFns {

  /**
   * Creates a single Avro file from a range of rows within a table
   *
   * @param table to export
   * @param bounds interval to export
   * @return AvroFile instance
   */
  Single<AvroFile> saveAsAvro(Table table, BoundedRange bounds, String pathExpr);

  /**
   * Creates a single Avro file from a table
   *
   * @param table to export
   * @return AvroFile instance
   */
  Single<AvroFile> saveAsAvro(Table table, String path);

  /**
   * Emits a ranges from a table which has been divided up
   *
   * @param table table to divide
   * @param divisions number of divisions within table
   * @return observable of range
   */
  Observable<BoundedRange> getTableRanges(final Table table, long divisions);

  /**
   * Clean up any temporary files or data after exporting a table
   *
   * @param table table that has finished export
   */
  void cleanup(final Table table);

}
