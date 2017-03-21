package co.cask.hydrator.plugin.batch.source;

import co.cask.hydrator.plugin.batch.ESProperties;
import co.cask.hydrator.plugin.batch.RecordWritableConverter;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import java.io.IOException;

/**
 * A {@link BatchSource} that writes data to Elasticsearch.
 * <p>
 * This {@link ElasticsearchSource} reads from an Elasticsearch index and type and converts the MapWritable
 * into a {@link StructuredRecord} and emits the StructuredRecord.
 * </p>
 * An exception will be thrown if the type of any of the fields do not match the type specified by the user.
 */
@Plugin(type = "batchsource")
@Name("Elasticsearch")
@Description("Elasticsearch Batch Source pulls documents from Elasticsearch " +
  "according to the query specified by the user and converts each document to a structured record ")
public class ElasticsearchSource extends ReferenceBatchSource<Text, MapWritable, StructuredRecord> {
  private static final String INDEX_DESCRIPTION = "The name of the index to query.";
  private static final String TYPE_DESCRIPTION = "The name of the type where the data is stored.";
  private static final String QUERY_DESCRIPTION = "The query to use to import data from the specified index. " +
    "See Elasticsearch for query examples.";
  private static final String HOST_DESCRIPTION = "The hostname and port for the Elasticsearch instance; " +
    "for example, localhost:9200.";
  private static final String SCHEMA_DESCRIPTION = "The schema or mapping of the data in Elasticsearch.";

  private final ESConfig config;
  private Schema schema;

  public ElasticsearchSource(ESConfig config) {
    super(config);
    this.config = config;
  }

  private String getResource() {
    return String.format("%s/%s", config.index, config.type);
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    schema = parseSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid output schema : " + e.getMessage(), e);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    job.setSpeculativeExecution(false);
    conf.set("es.nodes", config.hostname);
    conf.set("es.resource.read", getResource());
    conf.set("es.query", config.query);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(EsInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<Text, MapWritable> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(RecordWritableConverter.convertToRecord(input.getValue(), schema));
  }

  private Schema parseSchema() {
    try {
      return Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  /**
   * Config class for Batch {@link ElasticsearchSource}.
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

    @Name(ESProperties.QUERY)
    @Description(QUERY_DESCRIPTION)
    @Macro
    private String query;

    @Name(ESProperties.SCHEMA)
    @Description(SCHEMA_DESCRIPTION)
    private String schema;

    public ESConfig(String referenceName, String hostname, String index, String type, String query, String schema) {
      super(referenceName);
      this.hostname = hostname;
      this.index = index;
      this.type = type;
      this.schema = schema;
      this.query = query;
    }
  }
}
