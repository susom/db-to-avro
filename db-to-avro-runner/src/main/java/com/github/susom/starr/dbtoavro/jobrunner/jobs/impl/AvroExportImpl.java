package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.AvroExport;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroExportImpl extends AvroExport {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExportImpl.class);

  public AvroExportImpl(Database database, Config config) {
    super(database, config);
  }

  @Override
  public Observable<AvroFile> run(Job job, JobLogger logger) {
    return Observable.fromIterable(database.catalogs)
        .filter(catalog -> job.catalog.equals(catalog.name)) // filter out unwanted databases
        .flatMap(catalog -> Observable.fromIterable(catalog.schemas)
            .filter(schemas -> job.schemas.contains(schemas.name)) // filter out unwanted schemas
            .flatMap(schema -> Observable.fromIterable(schema.tables)
                .flatMap(table -> {
                      if (table.rows > rowsPerAvro) {
                        return avroFns.getRanges(table, table.rows / rowsPerAvro)
                            .toFlowable(BackpressureStrategy.BUFFER)
                            .parallel()
                            .runOn(Schedulers.computation())
                            .flatMap(range -> avroFns.saveAsAvro(table, range, job.destination + filePattern).toFlowable())
                            .sequential()
                            .doOnComplete(() -> avroFns.cleanup(table))
                            .toObservable()
                            .doOnNext(file -> logger.log(file.path))
                            .subscribeOn(Schedulers.io()); // run in io thread pool, while rows export in computation pool
                      } else {
                        return avroFns.saveAsAvro(table, job.destination + filePattern).toObservable()
                            .doOnNext(file -> logger.log(file.path))
                            .subscribeOn(Schedulers.io()); // run in io thread pool
                      }
                    }
                )
            )
        );
  }

}