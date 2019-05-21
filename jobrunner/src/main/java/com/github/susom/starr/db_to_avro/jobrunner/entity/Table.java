/*
 * Copyright 2019 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.github.susom.starr.db_to_avro.jobrunner.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.Serializable;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "name",
    "rows"
})
public class Table implements Serializable {

  private final static long serialVersionUID = 1025531320696832284L;
  @JsonProperty("name")
  private String name;
  @JsonProperty("rows")
  private Long rows;

  /**
   * No args constructor for use in serialization
   */
  public Table() {
  }

  /**
   *
   */
  public Table(String name, Long rows) {
    super();
    this.name = name;
    this.rows = rows;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  public Table withName(String name) {
    this.name = name;
    return this;
  }

  @JsonProperty("rows")
  public Long getRows() {
    return rows;
  }

  @JsonProperty("rows")
  public void setRows(Long rows) {
    this.rows = rows;
  }

  public Table withRows(Long rows) {
    this.rows = rows;
    return this;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(name).append(rows).toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Table) == false) {
      return false;
    }
    Table rhs = ((Table) other);
    return new EqualsBuilder().append(name, rhs.name).append(rows, rhs.rows).isEquals();
  }

}
