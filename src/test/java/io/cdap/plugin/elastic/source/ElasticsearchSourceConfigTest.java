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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.elastic.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link ElasticsearchSourceConfig}.
 */
public class ElasticsearchSourceConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final String REFERENCE_NAME = "referenceName";

  private static final ElasticsearchSourceConfig VALID_CONFIG = new ElasticsearchSourceConfig(
    "ReferenceName",
    "localhost:9200",
    "test",
    "test",
    "?q=*",
    Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG))).toString(),
    null
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidReferenceName() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setReferenceName("#")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, REFERENCE_NAME);
  }

  @Test
  public void testInvalidHostname() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:abc:abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidHostProtocol() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testValidHttpHostname() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("http://abc:9200")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidHttpsHostName() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("https://abc:9200")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testEmptyHostname() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testEmptyHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname(":9200")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testEmptyPort() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testEmptyPortWithHttpHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("http://abc:")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testEmptyPortWithHttpsHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("https://abc:")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidPort() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidPortWithHttpHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("http://abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidPortWithHttpsHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("https://abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidNumberInPort() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:100000")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidNumberInPortWithHttpHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("http://abc:100000")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidNumberInPortWithHttpsHost() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("https://abc:100000")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
  }

  @Test
  public void testInvalidIndex() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setIndex("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.INDEX_NAME);
  }

  @Test
  public void testInvalidType() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setType("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.TYPE_NAME);
  }

  @Test
  public void testInvalidQuery() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setQuery("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.QUERY);
  }

  @Test
  public void testEmptySchema() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setSchema("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.SCHEMA);
  }

  @Test
  public void testInvalidSchema() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setSchema("a")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.SCHEMA);
  }

  @Test
  public void testEmptyAdditionalProperties() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setAdditionalProperties("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidAdditionalProperties() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setAdditionalProperties("es.net.http.auth.user")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector,
      ElasticsearchSourceConfig.ADDITIONAL_PROPERTIES);
  }

  @Test
  public void testValidAdditionalProperties() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setAdditionalProperties("es.nodes.wan.only=true")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidAdditionalPropertiesWithEmptyKey() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setAdditionalProperties("es.nodes.wan.only=true;=false")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector,
      ElasticsearchSourceConfig.ADDITIONAL_PROPERTIES);
  }
}
