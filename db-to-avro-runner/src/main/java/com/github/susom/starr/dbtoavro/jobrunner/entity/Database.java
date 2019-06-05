package com.github.susom.starr.dbtoavro.jobrunner.entity;

import com.github.susom.database.Flavor;

/**
 * Simple pojo describing a database, running in docker
 */
public class Database {

  public String containerId;
  public Flavor flavor;

  public Database(String containerId) {
    this.containerId = containerId;
  }

}
