# Apache Beam examples
with Spotify Scio
and Google Cloud Product

### CSV to BigQuery Table runner

```sbt "runMain dev.rk.CsvToBigQuery
--project=[PROJECT_ID] --runner=DataflowRunner
--input=[CSV_LOCATION]
--zone=[ZONE]
--output=[DATASET].[TABLE]"```