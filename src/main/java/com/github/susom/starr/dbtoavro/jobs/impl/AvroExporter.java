package com.github.susom.starr.dbtoavro.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Column;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.functions.impl.FnFactory;
import com.github.susom.starr.dbtoavro.jobs.Exporter;
import com.github.susom.starr.dbtoavro.jobs.Loader;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroExporter implements Exporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExporter.class);

  private final Config config;
  private DatabaseProviderRx.Builder dbb;

  public AvroExporter(Config config, DatabaseProviderRx.Builder dbb) {
    this.config = config;
    this.dbb = dbb;
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {

    int threads = config.getIntegerOrThrow("database.pool.size");
    ExecutorService writerPool = Executors.newFixedThreadPool(threads);
    LOGGER.info("Starting export using {} threads", threads);

    return loader.run(job)
      .flatMapObservable(database -> {
        AvroFns avroFns = FnFactory.getAvroFns(database.flavor, job, dbb);
        DatabaseFns dbFns = FnFactory.getDatabaseFns(database.flavor, config, dbb);
        return
          dbFns.getSchemas(job.catalog)
            .filter(schema ->
              job.schemas.isEmpty()
                || job.schemas.stream().anyMatch(x -> x.equals(schema)))
            .flatMap(schema ->
              dbFns.getTables(job.catalog, schema, job.tablePriorities)
                .filter(table ->
                  job.tables.isEmpty()
                    || job.tables.stream().anyMatch(x -> x.equals(schema + "." + table))
                )
                .filter(table ->
                  job.tableExclusions.isEmpty()
                    || job.tableExclusions.stream()
                    .noneMatch(re -> (schema + "." + table).matches("(?i:" + re + ")"))
                )
                .flatMap(tableName ->
                  dbFns.introspect(job.catalog, schema, tableName, job.columnExclusions)
                    .flatMap(avroFns::saveAsAvro)
                    .subscribeOn(Schedulers.from(writerPool))
                    .toObservable()
                )
            );
      })
      .doOnComplete(writerPool::shutdown);
  }

}
