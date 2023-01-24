xjoin-validation
================

This is used to compare the contents of a database with the contents of an Elasticsearch index. The comparison is
performed in three stages:

1. [count] Compares the number of rows in the DB table with the number of records in the Elasticsearch index
2. [id] Compares the IDs in the DB table with the IDs in the Elasticsearch index
3. [content] Compares the entire contents of each row in the DB table with each record in the Elasticsearch index

There are a handful of parameters to configure the validation. These can be defined via environment variables or config
files.

### Dependencies

- go 1.18
- ginkgo

### Environment Variables

| Name                      | Description                                                    | Default Value                                                                                                                                                                           |
|---------------------------|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ELASTICSEARCH_HOST_URL    | Full URL to an Elasticsearch instance                          | http://xjoin-elasticsearch-es-default.test.svc:9200                                                                                                                                     |
| ELASTICSEARCH_USERNAME    | Elasticsearch instance username                                | xjoin                                                                                                                                                                                   |
| ELASTICSEARCH_PASSWORD    | Elasticsearch instance password                                | xjoin1337                                                                                                                                                                               |
| ELASTICSEARCH_INDEX       | Elasticsearch index to compare with a database                 | xjoinindexpipeline.hosts                                                                                                                                                                |
| FULL_AVRO_SCHEMA          | Avro schema that defines the structure of the data to validate | {}                                                                                                                                                                                      |
| <data-source>_DB_HOSTNAME | Hostname of the database used for <data-source>                | host-inventory-db.test.svc                                                                                                                                                              |
| <data-source>_DB_USERNAME | Username of the database used for <data-source>                | username                                                                                                                                                                                |
| <data-source>_DB_PASSWORD | Password of the database used for <data-source>                | password                                                                                                                                                                                |
| <data-source>_DB_NAME     | Name of the database used for <data-source>                    | host-inventory                                                                                                                                                                          |
| <data-source>_DB_PORT     | Port of the database used for <data-source>                    | 5432                                                                                                                                                                                    |
| <data-source>_DB_TABLE    | Table of the database used for <data-source>                   | hosts                                                                                                                                                                                   |
| <data-source>_DB_SSL_MODE | SSL_MODE for the database used for <data-source>               | disable                                                                                                                                                                                 |

### Running

Set each environment variable to the value specific to your environment before running xjoin-validation.

```shell
make run
```

### Running the tests

The tests use mocks, so they don't require a running instance of Elasticsearch or a database.

```shell
make test
```