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

package io.cdap.plugin.elastic;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

/**
 * Basic config class for Elasticsearch plugin.
 */
public abstract class BaseElasticsearchConfig extends ReferencePluginConfig {
  public static final String INDEX_NAME = "es.index";
  public static final String TYPE_NAME = "es.type";
  public static final String HOST = "es.host";

  private static final String HOST_DESCRIPTION = "The hostname and port for the Elasticsearch instance; " +
    "for example, localhost:9200.";
  private static final String INDEX_DESCRIPTION = "The name of the index to query.";
  private static final String TYPE_DESCRIPTION = "The name of the type where the data is stored.";

  @Name(HOST)
  @Description(HOST_DESCRIPTION)
  @Macro
  private final String hostname;

  @Name(INDEX_NAME)
  @Description(INDEX_DESCRIPTION)
  @Macro
  private final String index;

  @Name(TYPE_NAME)
  @Description(TYPE_DESCRIPTION)
  @Macro
  private final String type;

  public BaseElasticsearchConfig(String referenceName, String hostname, String index, String type) {
    super(referenceName);
    this.hostname = hostname;
    this.index = index;
    this.type = type;
  }

  public String getHostname() {
    return hostname;
  }

  public String getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }

  public String getResource() {
    return String.format("%s/%s", index, type);
  }

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);

    if (!containsMacro(HOST)) {
      if (Strings.isNullOrEmpty(hostname)) {
        collector.addFailure("Hostname must be specified.", null).withConfigProperty(HOST);
      } else {
        validateHost(collector);
      }
    }

    if (!containsMacro(INDEX_NAME) && Strings.isNullOrEmpty(index)) {
      collector.addFailure("Index must be specified.", null).withConfigProperty(INDEX_NAME);
    }

    if (!containsMacro(TYPE_NAME) && Strings.isNullOrEmpty(type)) {
      collector.addFailure("Type must be specified.", null).withConfigProperty(TYPE_NAME);
    }
  }

  private void validateHost(FailureCollector collector) {
    String[] hostParts = hostname.split(":");

    // Elasticsearch Hadoop does not support IPV6 https://github.com/elastic/elasticsearch-hadoop/issues/1105
    if (hostParts.length != 2) {
      collector.addFailure(
        "Invalid format of hostname",
        "Hostname and port must be specified for the Elasticsearch instance, for example: 'localhost:9200'"
      ).withConfigProperty(HOST);
    } else {
      String host = hostParts[0];
      String port = hostParts[1];

      if (host.isEmpty()) {
        collector.addFailure("Host should not be empty.", null)
          .withConfigProperty(HOST);
      }

      try {
        int portValue = Integer.parseInt(port);

        if (portValue < 0 || portValue > 65535) {
          collector.addFailure("Invalid port: " + port, "Port should be in range [0;65535]")
            .withConfigProperty(HOST);
        }
      } catch (NumberFormatException e) {
        collector.addFailure(
          "Invalid value for port: " + port,
          "Port should be a number in range [0;65535]")
          .withConfigProperty(HOST);
      }
    }
  }
}
