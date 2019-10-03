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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.elastic.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link ElasticsearchSinkConfig}.
 */
public class ElasticsearchSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final String REFERENCE_NAME = "referenceName";

  private static final ElasticsearchSinkConfig VALID_CONFIG = new ElasticsearchSinkConfig(
    "ReferenceName",
    "localhost:9200",
    "test",
    "test",
    "id"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidReferenceName() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setReferenceName("#")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, REFERENCE_NAME);
  }

  @Test
  public void testInvalidHostname() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.HOST);
  }

  @Test
  public void testEmptyHostname() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setHostname("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.HOST);
  }

  @Test
  public void testEmptyHost() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setHostname(":9200")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.HOST);
  }

  @Test
  public void testEmptyPort() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.HOST);
  }

  @Test
  public void testInvalidPort() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:abc")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.HOST);
  }

  @Test
  public void testInvalidNumberInPort() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setHostname("abc:100000")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.HOST);
  }

  @Test
  public void testInvalidIndex() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setIndex("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.INDEX_NAME);
  }

  @Test
  public void testInvalidType() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setType("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.TYPE_NAME);
  }

  @Test
  public void testInvalidIdField() {
    ElasticsearchSinkConfig config = ElasticsearchSinkConfig.newBuilder(VALID_CONFIG)
      .setIdField("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, ElasticsearchSinkConfig.ID_FIELD);
  }
}
