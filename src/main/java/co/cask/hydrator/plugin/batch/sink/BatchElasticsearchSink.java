package co.cask.hydrator.plugin.batch.sink;

import co.cask.hydrator.plugin.batch.ESProperties;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
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
public class BatchElasticsearchSink extends ReferenceBatchSink<StructuredRecord, Writable, Writable> {
  private static final String INDEX_DESCRIPTION = "The name of the index where the data will be stored. " +
    "If the index does not already exist, it will be created using Elasticsearch's default properties.";
  private static final String TYPE_DESCRIPTION = "The name of the type where the data will be stored. " +
    "If it does not already exist, it will be created.";
  private static final String ID_DESCRIPTION = "The field that will determine the id for the document. " +
    "It should match a fieldname in the structured record of the input.";
  private static final String HOST_DESCRIPTION = "The hostname and port for the Elasticsearch server; " +
    "such as localhost:9200.";
  private final ESConfig config;

  public BatchElasticsearchSink(ESConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws IOException {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    job.setSpeculativeExecution(false);

    conf.set("es.nodes", config.hostname);
    conf.set("es.resource", String.format("%s/%s", config.index, config.type));
    conf.set("es.input.json", "yes");
    conf.set("es.mapping.id", config.idField);

    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(EsOutputFormat.class, conf)));
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<Writable, Writable>> emitter) throws Exception {
    Text text = new Text(StructuredRecordStringConverter.toJsonString(record));
    emitter.emit(new KeyValue<Writable, Writable>(new Text(StructuredRecordStringConverter.toJsonString(record)),
                                                  new Text(StructuredRecordStringConverter.toJsonString(record))));
  }

  /**
   * Config class for BatchElasticsearchSink.java
   */
  public static class ESConfig extends ReferencePluginConfig {
    @Name(ESProperties.HOST)
    @Description(HOST_DESCRIPTION)
    @Macro
    private String hostname;

    @Name(ESProperties.INDEX_NAME)
    @Description(INDEX_DESCRIPTION)
    @Macro
    private String index;

    @Name(ESProperties.TYPE_NAME)
    @Description(TYPE_DESCRIPTION)
    @Macro
    private String type;

    @Name(ESProperties.ID_FIELD)
    @Description(ID_DESCRIPTION)
    @Macro
    private String idField;

    public ESConfig(String referenceName, String hostname, String index, String type, String idField) {
      super(referenceName);
      this.hostname = hostname;
      this.index = index;
      this.type = type;
      this.idField = idField;
    }
  }
}
