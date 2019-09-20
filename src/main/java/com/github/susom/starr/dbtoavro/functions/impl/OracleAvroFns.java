package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.database.Sql;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
  private boolean tidyTables;
  private boolean oracleStringDate;
  private String stringDateFormat;
  private String stringDateSuffix;

  public OracleAvroFns(Job job, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = job.fetchRows;
    this.codec = CodecFactory.fromString(job.codec);
    this.optimized = job.optimized;
    this.tidyTables = job.tidyTables;
    this.oracleStringDate = job.stringDate;
    this.stringDateFormat = job.stringDateFormat;
    this.stringDateSuffix = job.stringDateSuffix;
  }

  @Override
  public Observable<AvroFile> saveAsAvro(final Query query) {
    return dbb.transactRx(db -> {

      db.get().ddl("ALTER SESSION DISABLE PARALLEL QUERY").execute();
      db.get().ddl("ALTER SESSION SET \"_SERIAL_DIRECT_READ\" = TRUE").execute();

      String startTime = DateTime.now().toString();

      Etl.SaveAsAvro avro = Etl.saveQuery(db.get().toSelect(query.sql))
          .asAvro(query.path, query.table.schema, query.table.name)
          .withCodec(CodecFactory.snappyCodec())
          .withCodec(codec)
          .fetchSize(fetchSize);

      if (tidyTables) {
        avro = avro.tidyNames();
      }

      List<String> paths = new ArrayList<>();
      long rows = 0;
      if (query.batchSize > 0) {
        LOGGER.info("Writing {}", query.path);
        Map<String, Long> output = avro.start(query.batchSize);
        for (Map.Entry<String, Long> entry : output.entrySet()) {
          paths.add(entry.getKey());
          rows += entry.getValue();
        }
      } else {
        LOGGER.info("Writing {}", query.path);
        rows = avro.start();
        paths.add(query.path);
      }

      String endTime = DateTime.now().toString();

      return new AvroFile(query, paths, startTime, endTime, new File(query.path).length(), rows);

    }).toObservable();
  }

  @Override
  public Observable<Query> query(final Table table, final long targetSize, final String pathPattern) {

    // Only dump the supported column types
    String columns = table.columns.stream()
      .filter(c -> c.serializable)
      .map(c -> {
        // Use column name string (DATE) not java.sql.Type since JDBC is TIMESTAMP
        if (oracleStringDate && c.typeName.equals("DATE")) {
          return String.format("TO_CHAR(\"%s\", '%s') AS \"%s%s\"",
            c.name,
            stringDateFormat.replace(":", "::"),
            c.name,
            stringDateSuffix);
        } else {
          return "\"" + c.name + "\"";
        }
      })
      .collect(Collectors.joining(", "));

    String sql = String
        .format(Locale.CANADA, "SELECT %s FROM \"%s\".\"%s\"", columns, table.schema,
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
   * <p>High-performance table export using Oracle ROWID pseudo-column. Returns multiple queries for a single table,
   * partitioned by block ranges.</p>
   */
  @Override
  public Observable<Query> optimizedQuery(final Table table, final long targetSize, final String pathPattern) {

    // Check if table doesn't meet partitioning criteria, if not, bail.
    if (!optimized || table.bytes == 0 || table.bytes < (targetSize * 2) || targetSize == 0) {
      return Observable.empty();
    }

    // Only dump the supported column types
    String columns = table.columns.stream()
        .filter(c -> c.serializable)
        .map(c -> "\"" + c.name + "\"")
        .collect(Collectors.joining(", "));

    int partitions = (int) (table.bytes / targetSize);
    return getSegments(table, (partitions * 2) + 1)
        .map(segments -> {

          int splits = Math.min(segments.size(), partitions);

          LOGGER.info("Table {} ({} bytes) has {} chunks which will be merged into {} files", table.name, table.bytes,
              segments.size(), splits);

          int queriesPerFile = Math.floorDiv(segments.size(), splits);

          List<Query> queries = new ArrayList<>();
          List<String> subQueries = new ArrayList<>();

          int part = 0;
          int rr = 0;
          for (Segment segment : segments) {
            String sql = String
                .format(Locale.CANADA, "SELECT /*+ NO_INDEX(t) */ %s FROM \"%s\".\"%s\" WHERE "
                        + "(ROWID >= DBMS_ROWID.ROWID_CREATE(1, %d, %d, %d, 0)) AND "
                        + "(ROWID <= DBMS_ROWID.ROWID_CREATE(1, %d, %d, %d, 32767))",
                    columns, table.schema, table.name,
                    segment.id, segment.fileNo, segment.start,
                    segment.id, segment.fileNo, segment.end);
            subQueries.add(sql);
            if (rr >= queriesPerFile) {
              rr = 0;
              String path = pathPattern
                  .replace("%{CATALOG}", "ANY")
                  .replace("%{SCHEMA}", tidy(table.schema))
                  .replace("%{TABLE}", tidy(table.name))
                  .replace("%{PART}", String.format(Locale.CANADA, "%03d", part++));
              queries.add(new Query(table, String.join(" UNION ALL ", subQueries), 0, path));
              subQueries.clear();
            }
            rr++;

          }
          if (subQueries.size() > 0) {
            String path = pathPattern
                .replace("%{CATALOG}", "ANY")
                .replace("%{SCHEMA}", tidy(table.schema))
                .replace("%{TABLE}", tidy(table.name))
                .replace("%{PART}", String.format(Locale.CANADA, "%03d", part));
            queries.add(new Query(table, String.join(" UNION ALL ", subQueries), 0, path));
          }
          return queries;
        }).toObservable()
        .flatMapIterable(l -> l);

  }

  private String tidy(final String name) {
    if (tidyTables) {
      return name
          .replaceAll("[^a-zA-Z0-9]", " ")
          .replaceAll("\\s", "_")
          .trim()
          .toLowerCase(Locale.CANADA);
    } else {
      return name;
    }
  }

  private Maybe<List<Segment>> getSegments(final Table table, int batches) {
    return dbb.transactRx(db -> {
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

      List<Segment> segments = db.get().toSelect(sql).queryMany(rs -> new Segment(rs.getIntegerOrZero("DATA_OBJECT_ID"),
          rs.getIntegerOrZero("RELATIVE_FNO"),
          rs.getIntegerOrZero("START_BLOCK_ID"),
          rs.getIntegerOrZero("END_BLOCK_ID"),
          rs.getIntegerOrZero("FILE_BATCH")
      ));

      if (segments.size() == 0) {
        LOGGER.warn("Cannot split table {} (index-organized?)", table.name);
        return null; // Want an empty Maybe, not a Maybe containing an empty list
      }

      // The blocks get larger for each segment, we randomize so the file sizes are more consistent.
      Collections.shuffle(segments);

      return segments;

    });

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
