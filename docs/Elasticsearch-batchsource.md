# Elasticsearch Batch Source


Description
-----------
Pulls documents from Elasticsearch according to the query specified by the user and converts each document
to a Structured Record with the fields and schema specified by the user. The Elasticsearch server should
be running prior to creating the application.

This source is used whenever you need to read data from Elasticsearch. For example, you may want to read
in an index and type from Elasticsearch and store the data in an HBase table.


Configuration
-------------
**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**es.host:** The hostname and port for the Elasticsearch instance. (Macro-enabled)

**es.index:** The name of the index to query. (Macro-enabled)

**es.type:** The name of the type where the data is stored. (Macro-enabled)

**query:** The query to use to import data from the specified index and type;
see Elasticsearch for additional query examples. (Macro-enabled)

**schema:** The schema or mapping of the data in Elasticsearch.

**Additional Properties:** Additional properties to use with the es-hadoop client when reading the data, 
documented at [elastic.co](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html). 
(Macro-enabled)

Example
-------
This example connects to Elasticsearch, which is running locally, and reads in records in the
specified index (*megacorp*) and type (*employee*) which match the query to (in this case) select all records.
All data from the index will be read on each run:

    {
        "name": "Elasticsearch",
        "type": "batchsource",
        "properties": {
            "es.host": "localhost:9200",
            "es.index": "megacorp",
            "es.type": "employee",
            "query": "?q=*",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"etlSchemaBody\",
                \"fields\":[
                  {\"name\":\"id\",\"type\":\"long\"},
                  {\"name\":\"name\",\"type\":\"string\"},
                  {\"name\":\"age\",\"type\":\"int\"}]}"
        }
    }

This example connects to Elasticsearch, which is running in a remote restricted environment (e.g. elastic.co),
and reads in records in the specified index (*megacorp*) and type (*employee*) which match the query to 
(in this case) select all records. All data from the index will be read on each run:

    {
        "name": "Elasticsearch",
        "type": "batchsource",
        "properties": {
            "es.host": "https://remote.region.gcp.cloud.es.io:9243",
            "es.index": "megacorp",
            "es.type": "employee",
            "query": "?q=*",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"etlSchemaBody\",
                \"fields\":[
                  {\"name\":\"id\",\"type\":\"long\"},
                  {\"name\":\"name\",\"type\":\"string\"},
                  {\"name\":\"age\",\"type\":\"int\"}]}",
            "additionalProperties": "es.net.http.auth.user=username\nes.net.http.auth.pass=password\nes.nodes.wan.only=true"
        }
    }
