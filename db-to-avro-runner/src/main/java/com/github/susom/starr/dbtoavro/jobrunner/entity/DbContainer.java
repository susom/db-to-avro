package com.github.susom.starr.dbtoavro.jobrunner.entity;

import com.github.susom.database.Flavor;
import java.util.ArrayList;
import java.util.List;

public class DbContainer {

  public String containerId;
  public Flavor flavor;
  public List<FlatTable> tables = new ArrayList<>();

}
