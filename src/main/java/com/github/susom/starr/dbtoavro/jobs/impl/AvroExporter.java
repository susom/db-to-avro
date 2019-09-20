package com.github.susom.starr.dbtoavro.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.functions.impl.FnFactory;
import com.github.susom.starr.dbtoavro.jobs.Exporter;
import com.github.susom.starr.dbtoavro.jobs.Loader;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import java.nio.file.Paths;
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

    int threads = (int) (cores * (config.getDouble("avro.core.multiplier", 0.5)));

    String path = Paths.get(job.destination, filePattern).toString();

    ExecutorService dbPoolSched = Executors.newFixedThreadPool(threads);

    LOGGER.info("Starting export using {} threads", threads);

    return loader.run(job)
        .flatMapObservable(database -> {

          AvroFns avroFns = FnFactory.getAvroFns(database.flavor, job, dbb);
          DatabaseFns dbFns = FnFactory.getDatabaseFns(database.flavor, config, dbb);

          return
              Observable.just(job.schemas).flatMapIterable(l -> l)
                  .switchIfEmpty(dbFns.getSchemas(job.catalog))
                  .filter(schema -> job.exclusions.stream().noneMatch(s -> s.equals(schema)))
                  .flatMap(schema ->
                      Observable.just(job.tables).flatMapIterable(l -> l)
                          .switchIfEmpty(dbFns.getTables(job.catalog, schema))
                          .filter(table -> job.exclusions.stream().noneMatch(s -> s.equals(schema + "." + table)))
                          .flatMap(table ->
                              dbFns.introspect(job.catalog, schema, table, job.exclusions)
                                  .subscribeOn(Schedulers.from(dbPoolSched))
                          )
                          .flatMap(table ->
                              avroFns.optimizedQuery(table, job.avroSize, path)
                                  .flatMap(query ->
                                      avroFns.saveAsAvro(query)
                                          .subscribeOn(Schedulers.from(dbPoolSched))
                                  )
                                  .switchIfEmpty(
                                      avroFns.query(table, job.avroSize, path)
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
