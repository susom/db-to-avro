package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
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

    ExecutorService avroSched = Executors
        .newFixedThreadPool((int) (cores * (.75)));

    ExecutorService splitSched = Executors.newFixedThreadPool(cores > 4 ? (cores / 4) : 2);

    return loader.run(job)
        .flatMapObservable(database -> {
          AvroFns avroFns = FnFactory.getAvroFns(database.flavor, config);
          if (avroFns == null) {
            return Observable.error(new Exception("Unsupported database flavor!"));
          }
          return Observable.fromIterable(database.catalogs)
              .filter(catalog -> job.catalog.equals(catalog.name)) // filter out unwanted databases
              .flatMap(catalog -> Observable.fromIterable(catalog.schemas)
                  .filter(schemas -> job.schemas.contains(schemas.name)) // filter out unwanted schemas
                  .flatMap(schema -> Observable.fromIterable(schema.tables)
                      .filter(table -> table.bytes > threshold)
                      .flatMap(table -> {
                        if (table.bytes > threshold) {
                          return
                              avroFns.getTableRanges(table,
                                  Math.floorDiv(table.bytes, bytes) > 1 ? Math.floorDiv(table.bytes, bytes) : 2)
                                  .subscribeOn(Schedulers.from(splitSched))
                                  .toFlowable(BackpressureStrategy.BUFFER)
                                  .parallel()
                                  .runOn(Schedulers.from(avroSched))
                                  .flatMap(range -> avroFns.saveAsAvro(table, range, job.destination + filePattern)
                                      .toFlowable())
                                  .sequential()
                                  .doOnComplete(() -> avroFns.cleanup(table))
                                  .toObservable();
                        } else {
                          return avroFns.saveAsAvro(table, job.destination + filePattern)
                              .toObservable()
                              .subscribeOn(Schedulers.from(avroSched));
                        }
                      })
                  )
              );
        })
        .doOnComplete(avroSched::shutdown)
        .doOnComplete(splitSched::shutdown);
  }

}