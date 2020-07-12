/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.elastic.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.elastic.BaseElasticsearchConfig;

import java.io.IOException;

/**
 * Config class for {@link ElasticsearchSource}.
 */
public class ElasticsearchSourceConfig extends BaseElasticsearchConfig {
  public static final String QUERY = "query";
  public static final String SCHEMA = "schema";
  private static final String QUERY_DESCRIPTION = "The query to use to import data from the specified index. " +
    "See Elasticsearch for query examples.";
  private static final String SCHEMA_DESCRIPTION = "The schema or mapping of the data in Elasticsearch.";

  @Name(QUERY)
  @Description(QUERY_DESCRIPTION)
  @Macro
  private final String query;

  @Name(SCHEMA)
  @Description(SCHEMA_DESCRIPTION)
  private final String schema;

  public ElasticsearchSourceConfig(String referenceName, String hostname, String index, String type, String query,
                                   String schema, String additionalProperties) {
    super(referenceName, hostname, index, type, additionalProperties);
    this.schema = schema;
    this.query = query;
  }

  private ElasticsearchSourceConfig(Builder builder) {
    super(builder.referenceName, builder.hostname, builder.index, builder.type, builder.additionalProperties);
    query = builder.query;
    schema = builder.schema;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(ElasticsearchSourceConfig copy) {
    Builder builder = new Builder();
    builder.referenceName = copy.referenceName;
    builder.hostname = copy.getHostname();
    builder.index = copy.getIndex();
    builder.type = copy.getType();
    builder.query = copy.getQuery();
    builder.schema = copy.getSchema();
    builder.additionalProperties = copy.getAdditionalProperties();
    return builder;
  }

  public String getQuery() {
    return query;
  }

  public String getSchema() {
    return schema;
  }

  public Schema getParseSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(QUERY) && Strings.isNullOrEmpty(query)) {
      collector.addFailure("Query must be specified.", null).withConfigProperty(QUERY);
    }

    if (Strings.isNullOrEmpty(schema)) {
      collector.addFailure("Schema must be specified.", null).withConfigProperty(SCHEMA);
    } else {
      try {
        getParseSchema();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null)
          .withConfigProperty(ElasticsearchSourceConfig.SCHEMA);
      }
    }
  }

  /**
   * Builder for creating a {@link ElasticsearchSourceConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String hostname;
    private String index;
    private String type;
    private String query;
    private String schema;
    private String additionalProperties;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setIndex(String index) {
      this.index = index;
      return this;
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder setQuery(String query) {
      this.query = query;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder setAdditionalProperties(String additionalProperties) {
      this.additionalProperties = additionalProperties;
      return this;
    }

    public ElasticsearchSourceConfig build() {
      return new ElasticsearchSourceConfig(this);
    }
  }
}
