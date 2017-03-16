/*
 * Copyright © 2015 Cask Data, Inc.
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

package plugin.test;

import co.cask.hydrator.plugin.batch.ESProperties;
import co.cask.hydrator.plugin.batch.sink.BatchElasticsearchSink;
import co.cask.hydrator.plugin.batch.source.ElasticsearchSource;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import co.cask.hydrator.plugin.realtime.RealtimeElasticsearchSink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Unit test for co.cask.hydrator.plugin.batch {@link co.cask.hydrator.plugin.batch.sink.BatchElasticsearchSink} and {@link co.cask.hydrator.plugin.batch.source.ElasticsearchSource} classes.
 */
public class ETLESTest extends HydratorTestBase {

  private static final Schema TICKER_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.FLOAT)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final String VERSION = "3.2.0";
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion(VERSION);

  private static final ArtifactId BATCH_APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", VERSION);
  private static final ArtifactSummary BATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  private static final ArtifactId REALTIME_APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("etlrealtime", VERSION);
  private static final ArtifactSummary REALTIME_APP_ARTIFACT =
    new ArtifactSummary(REALTIME_APP_ARTIFACT_ID.getArtifact(), REALTIME_APP_ARTIFACT_ID.getVersion());

  private static final ArtifactRange REALTIME_ARTIFACT_RANGE = new ArtifactRange(NamespaceId.DEFAULT, "etlrealtime",
                                                                                 CURRENT_VERSION, true,
                                                                                 CURRENT_VERSION, true);
  private static final ArtifactRange BATCH_ARTIFACT_RANGE = new ArtifactRange(NamespaceId.DEFAULT, "data-pipeline",
                                                                              CURRENT_VERSION, true,
                                                                              CURRENT_VERSION, true);
  private Client client;
  private Node node;
  private int httpPort;
  private int transportPort;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl co.cask.hydrator.plugin.batch app and mocks for the co.cask.hydrator.plugin.batch app
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    //add the artifact for the etl co.cask.hydrator.plugin.realtime app and mocks for the co.cask.hydrator.plugin.realtime app
    setupRealtimeArtifacts(REALTIME_APP_ARTIFACT_ID, ETLRealtimeApplication.class);

    Set<ArtifactRange> parents = ImmutableSet.of(REALTIME_ARTIFACT_RANGE, BATCH_ARTIFACT_RANGE);

    // add elastic search plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("es-plugins", "1.0.0"), parents,
                      BatchElasticsearchSink.class, ElasticsearchSource.class, RealtimeElasticsearchSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    httpPort = Networks.getRandomPort();
    transportPort = Networks.getRandomPort();

    Settings settings = Settings.builder()
      .put("path.home", "target/elasticsearch")
      .put("path.data", temporaryFolder.newFolder("data"))
      .put("http.port", httpPort)
      .put("transport.tcp.port", transportPort)
      .put("cluster.name", "testcluster")
      .put("transport.type", "netty4")
      .put("http.enabled", "true")
      .put("http.type", "netty4").build();

    Collection plugins = Arrays.asList(Netty4Plugin.class);
    node = new PluginConfigurableNode(settings, plugins).start();

    client = node.client();
  }

  public static class PluginConfigurableNode extends Node {
    public PluginConfigurableNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null), classpathPlugins);
    }
  }

  @After
  public void afterTest() throws IOException {
    try {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("co/cask/hydrator/plugin/batch")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    } finally {
      node.close();
//      client.close();
    }
  }

  @Test
  public void testES() throws Exception {
    testBatchESSink();
    testESSource();
   // testRealtimeESSink();
  }

  private void testBatchESSink() throws Exception {
    String inputDatasetName = "input-batchsinktest";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));
    System.out.println("http port: " + httpPort);

    ETLStage sink = new ETLStage("Elasticsearch", new ETLPlugin(
      "Elasticsearch",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(ESProperties.HOST, "127.0.0.1" + ":" + httpPort,
                      ESProperties.INDEX_NAME, "co/cask/hydrator/plugin/batch",
                      ESProperties.TYPE_NAME, "testing",
                      ESProperties.ID_FIELD, "ticker",
                      Constants.Reference.REFERENCE_NAME, "BatchESSinkTest"),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("esSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(TICKER_SCHEMA).set("ticker", "AAPL").set("num", 10).set("price", 500.32).build(),
      StructuredRecord.builder(TICKER_SCHEMA).set("ticker", "CDAP").set("num", 13).set("price", 212.36).build()
    );
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    SearchResponse searchResponse = client.prepareSearch("co/cask/hydrator/plugin/batch").execute().actionGet();
    Assert.assertEquals(2, searchResponse.getHits().getTotalHits());
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "AAPL")).execute().actionGet();
    Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
    Assert.assertEquals("co/cask/hydrator/plugin/batch", searchResponse.getHits().getAt(0).getIndex());
    Assert.assertEquals("testing", searchResponse.getHits().getAt(0).getType());
    Assert.assertEquals("AAPL", searchResponse.getHits().getAt(0).getId());
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "ABCD")).execute().actionGet();
    Assert.assertEquals(0, searchResponse.getHits().getTotalHits());

    DeleteResponse response = client.prepareDelete("co/cask/hydrator/plugin/batch", "testing", "CDAP").execute().actionGet();
    Assert.assertEquals(response.status().getStatus(), 200);
  }

  @SuppressWarnings("ConstantConditions")
  private void testESSource() throws Exception {
    ETLStage source = new ETLStage("Elasticsearch", new ETLPlugin(
      "Elasticsearch",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ESProperties.HOST, "127.0.0.1" + ":" + httpPort)
        .put(ESProperties.INDEX_NAME, "co/cask/hydrator/plugin/batch")
        .put(ESProperties.TYPE_NAME, "testing")
        .put(ESProperties.QUERY, "?q=*")
        .put(ESProperties.SCHEMA, TICKER_SCHEMA.toString())
        .put(Constants.Reference.REFERENCE_NAME, "ESSourceTest").build(),
      null));
    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("esSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(1, outputRecords.size());
    StructuredRecord row1 = outputRecords.get(0);
    // Verify data
    Assert.assertEquals(10, (int) row1.get("num"));
    Assert.assertEquals(500.32f, (float) row1.get("price"), 0.0f);
    Assert.assertNull(row1.get("NOT_IMPORTED"));
  }

  private void testRealtimeESSink() throws Exception {

    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("time", Schema.of(Schema.Type.LONG))
    );
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("id", 1)
        .set("name", "Bob")
        .set("score", 3.4)
        .set("graduated", false)
        .set("binary", "Bob".getBytes(Charsets.UTF_8))
        .set("time", System.currentTimeMillis())
        .build()
    );
    ETLStage source = new ETLStage("source", co.cask.cdap.etl.mock.realtime.MockSource.getPlugin(input));

    ETLStage sink = new ETLStage("Elasticsearch", new ETLPlugin(
      "Elasticsearch",
      RealtimeSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(ESProperties.TRANSPORT_ADDRESSES, "127.0.0.1" + ":" + transportPort)
        .put(ESProperties.CLUSTER, "testcluster")
        .put(ESProperties.INDEX_NAME, "co/cask/hydrator/plugin/realtime")
        .put(ESProperties.TYPE_NAME, "testing")
        .put(ESProperties.ID_FIELD, "name")
        .put(Constants.Reference.REFERENCE_NAME, "ESSinkTest")
        .build(),
      null));
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    try {
      ApplicationId appId = NamespaceId.DEFAULT.app("testRealtimeSink");
      AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(REALTIME_APP_ARTIFACT, etlConfig);
      ApplicationManager appManager = deployApplication(appId, appRequest);

      WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

      workerManager.start();
      Tasks.waitFor(1L, new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          try {
            SearchResponse searchResponse = client.prepareSearch("co/cask/hydrator/plugin/realtime").execute().actionGet();
            return searchResponse.getHits().getTotalHits();
          } catch (Exception e) {
            //the index test won't exist until the run is finished
            return 0L;
          }
        }
      }, 15, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      workerManager.stop();

      SearchResponse searchResponse = client.prepareSearch("co/cask/hydrator/plugin/realtime").execute().actionGet();
      Map<String, Object> result = searchResponse.getHits().getAt(0).getSource();

      Assert.assertEquals(1, (int) result.get("id"));
      Assert.assertEquals("Bob", result.get("name"));
      Assert.assertEquals(3.4f,  (float) result.get("score"), 0.0f);
      Assert.assertEquals(false, result.get("graduated"));
      Assert.assertNotNull(result.get("time"));

      searchResponse = client.prepareSearch().setQuery(matchQuery("score", "3.4")).execute().actionGet();
      Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
      SearchHit hit = searchResponse.getHits().getAt(0);
      Assert.assertEquals("co/cask/hydrator/plugin/realtime", hit.getIndex());
      Assert.assertEquals("testing", hit.getType());
      Assert.assertEquals("Bob", hit.getId());
      searchResponse = client.prepareSearch().setQuery(matchQuery("name", "ABCD")).execute().actionGet();
      Assert.assertEquals(0, searchResponse.getHits().getTotalHits());

      DeleteResponse response = client.prepareDelete("co/cask/hydrator/plugin/realtime", "testing", "Bob").execute().actionGet();
      Assert.assertEquals(response.status().getStatus(), 200);
    } finally {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("co/cask/hydrator/plugin/realtime")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    }
  }
}
