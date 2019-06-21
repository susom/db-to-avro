package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.database.Sql;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Query;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Observable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleAvroFns.class);

  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean optimized;
  private boolean tidy;

  public OracleAvroFns(Config config, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = config.getInteger("avro.fetchsize", 10000);
    this.codec = CodecFactory.fromString(config.getString("avro.codec", "snappy"));
    this.optimized = config.getBooleanOrFalse("oracle.optimized.enable");
    this.tidy = config.getBooleanOrFalse("avro.tidy");
  }

  @Override
  public Observable<AvroFile> saveAsAvro(final Query query) {
    return dbb.withConnectionAccess().transactRx(db -> {

      db.get().ddl("ALTER SESSION DISABLE PARALLEL QUERY").execute();
      db.get().ddl("ALTER SESSION SET \"_SERIAL_DIRECT_READ\" = TRUE").execute();

      String startTime = DateTime.now().toString();

      List<String> paths = new ArrayList<>();
      if (query.rowsPerFile > 0) {
        LOGGER.info("Writing {}", query.path);
        paths.addAll(Etl.saveQuery(
            db.get().toSelect(query.sql))
            .asAvro(query.path, query.table.schema, query.table.name)
            .withCodec(CodecFactory.snappyCodec())
            .withCodec(codec)
            .fetchSize(fetchSize)
            .withTidy(tidy)
            .start(query.rowsPerFile));
      } else {
        LOGGER.info("Writing {}", query.path);
        Etl.saveQuery(
            db.get().toSelect(query.sql))
            .asAvro(query.path, query.table.schema, query.table.name)
            .withCodec(CodecFactory.snappyCodec())
            .withCodec(codec)
            .fetchSize(fetchSize)
            .withTidy(tidy)
            .start();
        paths.add(query.path);
      }

      String endTime = DateTime.now().toString();

      return new AvroFile(query, paths, startTime, endTime, new File(query.path).length());

    }).toObservable();
  }

  @Override
  public Observable<Query> query(final Table table, final long targetSize, final String pathPattern) {

    // Only dump the supported column types
    String columns = table.columns.stream()
        .filter(c -> c.serializable)
        .map(c -> c.name)
        .collect(Collectors.joining(", "));

    String sql = String
        .format(Locale.CANADA, "SELECT %s FROM %s.%s", columns, table.schema,
            table.name);

    String path;

    long rowsPerFile = 0;
    if (targetSize > 0 && table.bytes > 0 && table.rows > 0 && table.bytes > targetSize) {
      path = pathPattern
          .replace("%{CATALOG}", "ANY")
          .replace("%{SCHEMA}", tidy(table.schema))
          .replace("%{TABLE}", tidy(table.name));
      rowsPerFile = (targetSize) / (table.bytes / table.rows);
    } else {
      path = pathPattern
          .replace("%{CATALOG}", "ANY")
          .replace("%{SCHEMA}", tidy(table.schema))
          .replace("%{TABLE}", tidy(table.name))
          .replace("-%{PART}", "");

    }

    return Observable.just(new Query(table, sql, rowsPerFile, path));

  }


  /**
   * {@inheritDoc}
   * <p>Attempts to split table into partitions using the primary key(s).</p>
   * <p>This works best if the table primary keys are a clustered index.</p>
   * <p>If the table cannot be split, a single partition is emitted.</p>
   */
  @Override
  public Observable<Query> optimizedQuery(final Table table, final long targetSize, final String pathPattern) {

    // Only dump the supported column types
    String columns = table.columns.stream()
        .filter(c -> c.serializable)
        .map(c -> c.name)
        .collect(Collectors.joining(", "));

    return getSegments(table, 64)
        .map(segment -> {
          String path = pathPattern
              .replace("%{CATALOG}", "ANY")
              .replace("%{SCHEMA}", tidy(table.schema))
              .replace("%{TABLE}", tidy(table.name))
              .replace("%{PART}", String.format("%03d", segment.batch));
          String sql = String
              .format(Locale.CANADA, "SELECT /*+ NO_INDEX(t) */ %s FROM %s.%s WHERE "
                      + "(ROWID >= DBMS_ROWID.ROWID_CREATE(1, %d, %d, %d, 0)) AND "
                      + "(ROWID <= DBMS_ROWID.ROWID_CREATE(1, %d, %d, %d, 32767))",
                  columns, table.schema, table.name,
                  segment.id, segment.fileNo, segment.start,
                  segment.id, segment.fileNo, segment.end);
          LOGGER.debug("SQL: {}", sql);
          return new Query(table, sql, 0, path);
        });

  }

  private String tidy(final String name) {
    if (tidy) {
      return name
          .replaceAll("[^a-zA-Z0-9]", " ")
          .replaceAll("\\s", "_")
          .trim()
          .toLowerCase();
    } else {
      return name;
    }
  }


  /* TODO: Where left off: Batches might be less than # of splits -- so need to do a round-robin allocation.
  TODO: also the block sizes are not the same.. do they need to be added together so block sizes are unioned?
  What was going on there?    Oraoopdatadrivendbinputformat:282

   */



  private Observable<Segment> getSegments(final Table table, int batches) {
    return dbb.withConnectionAccess().transactRx(db -> {
      db.get().underlyingConnection().setSchema(table.schema);

      Sql sql = new Sql();
      sql.append("SELECT DATA_OBJECT_ID,\n"
          + "       FILE_ID,\n"
          + "       RELATIVE_FNO,\n"
          + "       FILE_BATCH,\n"
          + "       MIN(START_BLOCK_ID) START_BLOCK_ID,\n"
          + "       MAX(END_BLOCK_ID)   END_BLOCK_ID,\n"
          + "       SUM(BLOCKS)         BLOCKS\n"
          + "FROM (SELECT O.DATA_OBJECT_ID,\n"
          + "             E.FILE_ID,\n"
          + "             E.RELATIVE_FNO,\n"
          + "             E.BLOCK_ID START_BLOCK_ID,\n"
          + "             E.BLOCK_ID + E.BLOCKS - 1 END_BLOCK_ID,\n"
          + "             E.BLOCKS,\n"
          + "             CEIL(\n"
          + "                         SUM(\n"
          + "                                 E.BLOCKS)\n"
          + "                                 OVER (PARTITION BY O.DATA_OBJECT_ID, E.FILE_ID\n"
          + "                                     ORDER BY E.BLOCK_ID ASC)\n"
          + "                         / (SUM(E.BLOCKS)\n"
          + "                                OVER (PARTITION BY O.DATA_OBJECT_ID, E.FILE_ID)\n"
          + "                         / :batches))\n"
          + "                        FILE_BATCH\n"
          + "      FROM DBA_EXTENTS E,\n"
          + "           DBA_OBJECTS O,\n"
          + "           DBA_TAB_SUBPARTITIONS TSP\n"
          + "      WHERE O.OWNER = :schema\n"
          + "        AND O.OBJECT_NAME = :table\n"
          + "        AND E.OWNER = :schema\n"
          + "        AND E.SEGMENT_NAME = :table\n"
          + "        AND O.OWNER = E.OWNER\n"
          + "        AND O.OBJECT_NAME = E.SEGMENT_NAME\n"
          + "        AND (O.SUBOBJECT_NAME = E.PARTITION_NAME\n"
          + "          OR (O.SUBOBJECT_NAME IS NULL AND E.PARTITION_NAME IS NULL))\n"
          + "        AND O.OWNER = TSP.TABLE_OWNER(+)\n"
          + "        AND O.OBJECT_NAME = TSP.TABLE_NAME(+)\n"
          + "        AND O.SUBOBJECT_NAME = TSP.SUBPARTITION_NAME(+)\n"
          + "     )\n"
          + "GROUP BY DATA_OBJECT_ID, FILE_ID,\n"
          + "         RELATIVE_FNO, FILE_BATCH\n"
          + "ORDER BY DATA_OBJECT_ID, FILE_ID,\n"
          + "         RELATIVE_FNO, FILE_BATCH")
          .argInteger("batches", batches)
          .argString("schema", table.schema)
          .argString("table", table.name);

      return db.get().toSelect(sql).queryMany(rs -> new Segment(rs.getIntegerOrZero("DATA_OBJECT_ID"),
          rs.getIntegerOrZero("RELATIVE_FNO"),
          rs.getIntegerOrZero("START_BLOCK_ID"),
          rs.getIntegerOrZero("END_BLOCK_ID"),
          rs.getIntegerOrZero("FILE_BATCH")
      ));

    }).toObservable().flatMapIterable(l -> l);

  }

  private class Segment {

    private int id;
    private int fileNo;
    private long start;
    private long end;
    private int batch;

    public Segment(int id, int fileNo, long start, long end, int batch) {
      this.id = id;
      this.fileNo = fileNo;
      this.start = start;
      this.end = end;
      this.batch = batch;
    }
  }

}