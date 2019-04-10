package io.cdap.plugin.batch;

/**
 * Constants for ElasticSearch plugins.
 */
public class ESProperties {
  public static final String INDEX_NAME = "es.index";
  public static final String TYPE_NAME = "es.type";
  public static final String HOST = "es.host";
  public static final String QUERY = "query";
  public static final String SCHEMA = "schema";

  public static final String ID_FIELD = "es.idField";
  public static final String TRANSPORT_ADDRESSES = "es.transportAddresses";
  public static final String CLUSTER = "es.cluster";

  private ESProperties() {
  }
}
