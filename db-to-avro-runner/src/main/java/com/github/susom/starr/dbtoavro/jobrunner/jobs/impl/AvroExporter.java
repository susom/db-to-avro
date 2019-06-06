package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.FnFactory;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Exporter;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroExporter implements Exporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExporter.class);

  private final Config config;
  private final String filePattern;
  private final long threshold;
  private final long bytes;
  private final int cores;

  public AvroExporter(Config config) {
    this.config = config;
    this.threshold = config.getLong("avro.split.threshold", 1000000000);
    this.bytes = config.getLong("avro.split.bytes", 250000000);
    this.filePattern = config.getString("avro.filename", "%{SCHEMA}.%{TABLE}-%{PART}.avro");
    this.cores = Runtime.getRuntime().availableProcessors();
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {

    int threads = (int) (cores * (.50));

    ExecutorService dbPoolSched = Executors.newFixedThreadPool(threads);
    LOGGER.info("Starting export using {} threads", threads);

    return loader.run(job)
        .flatMapObservable(database -> {
          AvroFns avroFns = FnFactory.getAvroFns(database.flavor, config);
          DatabaseFns dbFns = FnFactory.getDatabaseFns(database.flavor, config);
          if (avroFns == null || dbFns == null) {
            return Observable.error(new Exception("Unsupported database flavor!"));
          }
          return dbFns.getCatalogs(database)
              .filter(job.catalog::equals) // filter out unwanted databases
              .flatMap(catalog -> dbFns.getSchemas(catalog)
                  .filter(job.schemas::contains) // filter out unwanted schemas
                  .flatMap(schema -> dbFns.getTables(catalog, schema)
                      .flatMap(t -> Observable.just(t)
                          .flatMapSingle(dbFns::introspect)
                          .subscribeOn(Schedulers.from(dbPoolSched))
                      )
                      .flatMap(table -> avroFns.getSplitterColumn(table, threshold)
                          .flatMapObservable(d -> avroFns.getTableRanges(table, d,
                              Math.floorDiv(table.bytes, bytes) > 1 ? Math.floorDiv(table.bytes, bytes) : 2)
                              .subscribeOn(Schedulers.from(dbPoolSched))
                              .toFlowable(BackpressureStrategy.BUFFER)
                              .parallel()
                              .runOn(Schedulers.from(dbPoolSched))
                              .flatMap(
                                  range -> avroFns.saveAsAvro(table, range, job.destination + filePattern)
                                      .toFlowable())
                              .sequential().toObservable()
                          ).switchIfEmpty(
                              avroFns.saveAsAvro(table, job.destination + filePattern)
                                  .toObservable()
                                  .subscribeOn(Schedulers.from(dbPoolSched))
                          )
                      )
                  )
              );
        })
        .doOnComplete(dbPoolSched::shutdown);
  }

}
