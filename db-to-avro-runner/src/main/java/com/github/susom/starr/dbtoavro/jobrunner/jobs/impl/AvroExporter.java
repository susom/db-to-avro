package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.FnFactory;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Exporter;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
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
  private final int cores;
  private DatabaseProviderRx.Builder dbb;

  public AvroExporter(Config config, DatabaseProviderRx.Builder dbb) {
    this.config = config;
    this.dbb = dbb;
    this.filePattern = config.getString("avro.filename", "%{SCHEMA}.%{TABLE}-%{PART}.avro");
    this.cores = Runtime.getRuntime().availableProcessors();
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {

    long targetSize = config.getLong("avro.targetsize", 0);
    int threads = (int) (cores * (config.getDouble("avro.core.multiplier", 0.5)));
    String path = job.destination + filePattern;

    ExecutorService dbPoolSched = Executors.newFixedThreadPool(threads);

    LOGGER.info("Starting export using {} threads", threads);

    return loader.run(job)
        .flatMapObservable(database -> {

          AvroFns avroFns = FnFactory.getAvroFns(database.flavor, config, dbb);
          DatabaseFns dbFns = FnFactory.getDatabaseFns(database.flavor, config, dbb);

          return dbFns.getSchemas(job.catalog)
              .filter(job.schemas::contains) // filter out unwanted schemas
              .flatMap(schema ->

                  Observable.just(job.tables)
                      .flatMapIterable(l -> l)
                      .switchIfEmpty(dbFns.getTables(job.catalog, schema))

                      .flatMap(table -> dbFns.introspect(job.catalog, schema, table)
                          .subscribeOn(Schedulers.from(dbPoolSched)))
                      .flatMap(table ->

                          avroFns.optimizedQuery(table, targetSize, path)
                              .flatMap(query ->
                                  avroFns.saveAsAvro(query)
                                      .subscribeOn(Schedulers.from(dbPoolSched))
                              )
                              .switchIfEmpty(
                                  avroFns.query(table, targetSize, path)
                                      .flatMap(query ->
                                          avroFns.saveAsAvro(query)
                                              .subscribeOn(Schedulers.from(dbPoolSched))
                                      )
                              )

                      )

              );
        })
        .doOnComplete(dbPoolSched::shutdown);
  }

}
