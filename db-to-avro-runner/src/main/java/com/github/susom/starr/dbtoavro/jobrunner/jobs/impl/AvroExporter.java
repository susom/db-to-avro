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
  private final int avroRows;
  private final long threshold;
  private final int cores;

  public AvroExporter(Config config) {
    this.config = config;
    this.avroRows = config.getInteger("avro.rows", 100000);
    this.threshold = config.getLong("avro.split.threshold", 25000000);
    this.filePattern = config.getString("avro.filename", "%{SCHEMA}.%{TABLE}-%{PART}.avro");
    this.cores = Runtime.getRuntime().availableProcessors();
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {

    // Create schedulers for various tasks based on number of cores
    ExecutorService smallTblSched = Executors.newFixedThreadPool(cores);
    ExecutorService splitTblSched = Executors.newFixedThreadPool(cores >= 4 ? cores / 4 : 2);
    ExecutorService copyTblSched = Executors.newFixedThreadPool(cores >= 8 ? cores / 8 : 2);

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
                      .flatMap(table -> {
                        if (table.bytes > threshold) {
                          return
                              avroFns.getRanges(table,
                                  Math.floorDiv(table.bytes, threshold) > 1 ? Math.floorDiv(table.bytes, threshold) : 2)
                                  .subscribeOn(Schedulers.from(copyTblSched))
                                  .toFlowable(BackpressureStrategy.BUFFER)
                                  .parallel()
                                  .runOn(Schedulers.from(splitTblSched))
                                  .flatMap(
                                      range -> avroFns.saveAsAvro(table, range, job.destination + filePattern)
                                          .toFlowable())
                                  .sequential()
                                  .doOnComplete(() -> avroFns.cleanup(table))
                                  .toObservable();
                        } else {
                          return avroFns.saveAsAvro(table, job.destination + filePattern)
                              .toObservable()
                              .subscribeOn(Schedulers.from(smallTblSched));
                        }
                      })
                  )
              );
        })
        .doOnComplete(smallTblSched::shutdown)
        .doOnComplete(splitTblSched::shutdown)
        .doOnComplete(copyTblSched::shutdown);
  }

}