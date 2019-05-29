package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Pojo representing a window of rows within a table
 */
public class Range {

  public int index;
  public long start;
  public long end;
  public boolean terminal; // hint for final sql to used a closed 'where'

  public Range(int index, long start, long end, boolean terminal) {
    this.index = index;
    this.start = start;
    this.end = end;
    this.terminal = terminal;
  }

  public Range(int index, long start, long end) {
    this.index = index;
    this.start = start;
    this.end = end;
  }


}
