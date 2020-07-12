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

package io.cdap.plugin.elastic.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.elastic.BaseElasticsearchConfig;

/**
 * Config class for {@link BatchElasticsearchSink}.
 */
public class ElasticsearchSinkConfig extends BaseElasticsearchConfig {
  public static final String ID_FIELD = "es.idField";

  private static final String ID_DESCRIPTION = "The field that will determine the id for the document. " +
    "It should match a fieldname in the structured record of the input.";

  @Name(ID_FIELD)
  @Description(ID_DESCRIPTION)
  @Macro
  private final String idField;

  public ElasticsearchSinkConfig(String referenceName, String hostname, String index, String type, String idField,
                                 String additionalProperties) {
    super(referenceName, hostname, index, type, additionalProperties);
    this.idField = idField;
  }

  private ElasticsearchSinkConfig(Builder builder) {
    super(builder.referenceName, builder.hostname, builder.index, builder.type, builder.additionalProperties);
    idField = builder.idField;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(ElasticsearchSinkConfig copy) {
    Builder builder = new Builder();
    builder.referenceName = copy.referenceName;
    builder.hostname = copy.getHostname();
    builder.index = copy.getIndex();
    builder.type = copy.getType();
    builder.idField = copy.getIdField();
    builder.additionalProperties = copy.getAdditionalProperties();
    return builder;
  }

  public String getIdField() {
    return idField;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(ID_FIELD) && Strings.isNullOrEmpty(idField)) {
      collector.addFailure("ID field must be specified.", null).withConfigProperty(ID_FIELD);
    }
  }

  /**
   * Builder for creating a {@link ElasticsearchSinkConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String hostname;
    private String index;
    private String type;
    private String idField;
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

    public Builder setIdField(String idField) {
      this.idField = idField;
      return this;
    }

    public Builder setAdditionalProperties(String additionalProperties) {
      this.additionalProperties = additionalProperties;
      return this;
    }

    public ElasticsearchSinkConfig build() {
      return new ElasticsearchSinkConfig(this);
    }
  }
}
