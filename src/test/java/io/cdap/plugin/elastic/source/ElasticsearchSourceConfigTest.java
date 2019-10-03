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
    Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG))).toString()
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
      .setHostname("abc:abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSourceConfig.HOST);
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
  public void testInvalidPort() {
    ElasticsearchSourceConfig config = ElasticsearchSourceConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:abc")
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
}
