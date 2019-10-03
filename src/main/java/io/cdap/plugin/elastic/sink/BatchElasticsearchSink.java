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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

/**
 * A {@link BatchSink} that writes data to a Elasticsearch.
 * <p/>
 * This {@link BatchElasticsearchSink} takes a {@link StructuredRecord} in,
 * converts it to a json per {@link StructuredRecordStringConverter},
 * and writes it to the Elasticsearch server.
 * <p/>
 * If the Elasticsearch index does not exist, it will be created using the default properties
 * specified by Elasticsearch. See more information at
 * https://www.elastic.co/guide/en/elasticsearch/guide/current/_index_settings.html.
 * <p/>
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Elasticsearch")
@Description("Elasticsearch Batch Sink takes the structured record from the input source and converts it " +
  "to a JSON string, then indexes it in Elasticsearch using the index, type, and id specified by the user.")
public class BatchElasticsearchSink extends BatchSink<StructuredRecord, Writable, Writable> {
  private final ElasticsearchSinkConfig config;

  public BatchElasticsearchSink(ElasticsearchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws IOException {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    job.setSpeculativeExecution(false);

    conf.set("es.nodes", config.getHostname());
    conf.set("es.resource.write", config.getResource());
    conf.set("es.input.json", "yes");
    conf.set("es.mapping.id", config.getIdField());

    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(EsOutputFormat.class, conf)));
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<Writable, Writable>> emitter) throws Exception {
    emitter.emit(new KeyValue<Writable, Writable>(new Text(StructuredRecordStringConverter.toJsonString(record)),
                                                  new Text(StructuredRecordStringConverter.toJsonString(record))));
  }
}
