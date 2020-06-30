# Elasticsearch Batch Sink


Description
-----------
Takes the Structured Record from the input source and converts it to a JSON string, then indexes it in
Elasticsearch using the index, type, and idField specified by the user. The Elasticsearch server should
be running prior to creating the application.

This sink is used whenever you need to write to an Elasticsearch server. For example, you
may want to parse a file and read its contents into Elasticsearch, which you can achieve
with a stream batch source and Elasticsearch as a sink.


Configuration
-------------
**Reference Name:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**es.host:** The hostname and port for the Elasticsearch instance. (Macro-enabled)

**es.index:** The name of the index where the data will be stored; if the index does not
already exist, it will be created using Elasticsearch's default properties. (Macro-enabled)

**es.type:** The name of the type where the data will be stored; if it does not already
exist, it will be created. (Macro-enabled)

**es.idField:** The field that will determine the id for the document; it should match a fieldname
in the Structured Record of the input. (Macro-enabled)

**Additional Properties:** Additional properties to use with the es-hadoop client when writing the data, 
documented at [elastic.co](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html). 
(Macro-enabled)

Example
-------
This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (megacorp) and type (employee). The data is indexed using the id field
in the record. Each run, the documents will be updated if they are still present in the source:

    {
        "name": "Elasticsearch",
        "type": "batchsink",
        "properties": {
            "es.host": "localhost:9200",
            "es.index": "megacorp",
            "es.type": "employee",
            "es.idField": "id"
        }
    }

This example connects to Elasticsearch, which is running in a remote restricted environment (e.g. elastic.co), 
and writes the data to the specified index (megacorp) and type (employee). The data is indexed using the id field
in the record. Each run, the documents will be updated if they are still present in the source:

    {
        "name": "Elasticsearch",
        "type": "batchsink",
        "properties": {
            "es.host": "https://remote.region.gcp.cloud.es.io:9243",
            "es.index": "megacorp",
            "es.type": "employee",
            "es.idField": "id",
            "additionalProperties": "es.net.http.auth.user=username\nes.net.http.auth.pass=password\nes.nodes.wan.only=true"
        }
    }
