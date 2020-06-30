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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.elastic.RecordWritableConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsInputFormat;

/**
 * A {@link BatchSource} that writes data to Elasticsearch.
 * <p>
 * This {@link ElasticsearchSource} reads from an Elasticsearch index and type and converts the MapWritable
 * into a {@link StructuredRecord} and emits the StructuredRecord.
 * </p>
 * An exception will be thrown if the type of any of the fields do not match the type specified by the user.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Elasticsearch")
@Description("Elasticsearch Batch Source pulls documents from Elasticsearch " +
  "according to the query specified by the user and converts each document to a structured record ")
public class ElasticsearchSource extends BatchSource<Text, MapWritable, StructuredRecord> {
  private final ElasticsearchSourceConfig config;
  private Schema schema;

  public ElasticsearchSource(ElasticsearchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    schema = config.getParseSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    config.validate(collector);
    collector.getOrThrowException();

    Schema schema = config.getParseSchema();
    stageConfigurer.setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    job.setSpeculativeExecution(false);
    conf.set("es.nodes", config.getHostname());
    conf.set("es.resource.read", config.getResource());
    conf.set("es.query", config.getQuery());
    config.getAdditionalPropertiesMap().forEach((k, v) -> conf.set(k, v));
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(EsInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<Text, MapWritable> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(RecordWritableConverter.convertToRecord(input.getValue(), schema));
  }
}
