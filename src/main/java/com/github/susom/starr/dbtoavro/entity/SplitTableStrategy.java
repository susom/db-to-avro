package com.github.susom.starr.dbtoavro.entity;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import java.util.StringTokenizer;

/**
 * Split Table Option/strategy for a given table to be split
 */
public class SplitTableStrategy {

  private String tableName;
  private String column;
  private List<String> columns;
  private SplitTableStrategyOperation operation;
  private SplitTableStrategyOperation subOperation;
  private String subColumn;

  private Long increment;
  private Long startRange;
  private Long endRange;
  private String prefix;
  private KeyDataType keyDataType;
  private String startRangeString;
  private String endRangeString;
  private Date startRangeDate;
  private Date endRangeDate;

  //table_name, operation, column(s), keyDataType (optional), increment (optional), start_range (optional), end_range (optional), prefix (optional)
  public SplitTableStrategy(String x) {    

    //List<String> data = Collections.list(new StringTokenizer(((String)x), ",")).stream().map(token -> (String) token).collect(Collectors.toList());
    List<String> data = Arrays.stream(x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)).collect(Collectors.toList());

    if (data.size() >= 3) { 
      int index = 0;
      this.tableName = data.get(index).trim(); //index = 0
      this.operation = SplitTableStrategyOperation.valueOf(data.get(++index).trim()); //index = 1
      this.column = data.get(++index).trim(); //index = 2
      if (this.operation == SplitTableStrategyOperation.query) {
        this.column = this.column.replaceAll("\"", "");
        this.columns = this.column.contains(";") ?
          Collections.list(new StringTokenizer(column, ";")).stream().map(token -> (String) token).collect(Collectors.toList()) : Arrays.asList(column);
        this.column = null;
        if (data.size() > (index + 2)) {
          this.subOperation = SplitTableStrategyOperation.valueOf(data.get(++index).trim()); //index = 3
          this.subColumn = data.get(++index).trim(); //index = 4
        } 
      }
      if (data.size() > (index + 1)) { //index + 1 = 3
        this.keyDataType = KeyDataType.valueOf(data.get(++index).trim()); //index = 3
        if (data.size() > (index + 1)) { //index + 1 = 4
          this.increment = Long.parseLong(data.get(++index).trim()); //index = 4
          if (data.size() >= (index + 2)) { //index = 6
            if (keyDataType == KeyDataType.number) {
              this.startRange = Long.parseLong(data.get(++index).trim()); //index = 5
              this.endRange = Long.parseLong(data.get(++index).trim()); //index = 6
            }
            else {
              this.startRangeString = data.get(++index).trim(); //index = 5
              this.endRangeString = data.get(++index).trim(); //index = 6
            }
            if (data.size() > (index + 1)) { //index + 1 = 7
              this.prefix = data.get(++index).trim(); //index = 7
            }
          }
        }
      }
    }

  }

  public String getTableName() {
    return tableName;
  }

  public Long getStartRange() {
    return startRange;
  }

  public void setStartRange(Long startRange) {
    this.startRange = startRange;
  }

  public Long getEndRange() {
    return endRange;
  }

  public void setEndRange(Long endRange) {
    this.endRange = endRange;
  }

  public String getStartRangeString() {
    return startRangeString;
  }

  public void setStartRangeString(String startRange) {
    this.startRangeString = startRange;
  }

  public String getEndRangeString() {
    return endRangeString;
  }

  public void setEndRangeString(String endRange) {
    this.endRangeString = endRange;
  }

  public Date getStartRangeDate() {
    return startRangeDate;
  }

  public void setStartRangeDate(Date startRange) {
    this.startRangeDate = startRange;
  }

  public Date getEndRangeDate() {
    return endRangeDate;
  }

  public void setEndRangeDate(Date endRange) {
    this.endRangeDate = endRange;
  }

  public SplitTableStrategyOperation getOperation() {
    return operation;
  }

  public KeyDataType getKeyDataType() {
    return keyDataType;
  }

  public String getColumn() {
    return column;
  }

  public List<String> getColumns() {
    return columns;
  }

  public Long getIncrement() {
    return increment;
  }

  public void setIncrement(Long increment) {
    this.increment = increment;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getSubColumn() {
    return subColumn;
  }

  public void setSubColumn(String subColumn) {
    this.subColumn = subColumn;
  }

  public SplitTableStrategyOperation getSubOperation() {
    return subOperation;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "tableName=%s, column=%s, keyDataType=%s, operation=%s, increment=%s, startRange=%s, endRange=%s, startRangeString=%s, endRangeString=%s, startRangeDate=%s, endRangeDate=%s, prefix=%s, subOperation=%s, subColumn=%s",
      tableName,
      column, 
      keyDataType, 
      operation, 
      increment, 
      startRange,
      endRange, 
      startRangeString,
      endRangeString, 
      startRangeDate,
      endRangeDate,
      prefix,
      subOperation,
      subColumn);
  }
}
