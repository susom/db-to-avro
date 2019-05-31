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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroExporter implements Exporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExporter.class);

  private final Config config;
  private final String filePattern;
  private final int avroRows;

  public AvroExporter(Config config) {
    this.config = config;
    this.avroRows = config.getInteger("avro.rows", 100000);
    this.filePattern = config.getString("avro.filename", "%{SCHEMA}.%{TABLE}-%{PART}.avro");
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {
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
                            if (table.rows > avroRows) {
                              return avroFns.getRanges(table, table.rows / avroRows)
                                  .toFlowable(BackpressureStrategy.BUFFER)
                                  .parallel()
                                  .runOn(Schedulers.computation())
                                  .flatMap(
                                      range -> avroFns.saveAsAvro(table, range, job.destination + filePattern).toFlowable())
                                  .sequential()
                                  .doOnComplete(() -> avroFns.cleanup(table))
                                  .toObservable()
                                  .doOnNext(file -> LOGGER.info("Wrote {}", file.path))
                                  .subscribeOn(
                                      Schedulers
                                          .computation()); // run in io thread pool, while rows export in computation pool
                            } else {
                              return avroFns.saveAsAvro(table, job.destination + filePattern).toObservable()
                                  .doOnNext(file -> LOGGER.info("Wrote {}", file.path))
                                  .subscribeOn(Schedulers.computation()); // run in io thread pool
                            }
                          }
                      )
                  )
              );
        });
  }

}