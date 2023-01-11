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

| Name                   | Description                                                    | Default Value                                                                                                                                                                           |
|------------------------|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ELASTICSEARCH_HOST_URL | Full URL to an Elasticsearch instance                          | http://xjoin-elasticsearch-es-default.test.svc:9200                                                                                                                                     |
| ELASTICSEARCH_USERNAME | Elasticsearch instance username                                | xjoin                                                                                                                                                                                   |
| ELASTICSEARCH_PASSWORD | Elasticsearch instance password                                | xjoin1337                                                                                                                                                                               |
| ELASTICSEARCH_INDEX    | Elasticsearch index to compare with a database                 | xjoinindexpipeline.hosts                                                                                                                                                                |
| FULL_AVRO_SCHEMA       | Avro schema that defines the structure of the data to validate | {}                                                                                                                                                                                      |
| DATABASE_CONNECTIONS   | JSON containing connection info for each DataSource's DB       | {"hosts": {"sslMode": "disable", "name": "host-inventory", "hostname": "host-inventory-db.test.svc", "password": "password", "username": "username", "table": "hosts", "port": "5432"}} |

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