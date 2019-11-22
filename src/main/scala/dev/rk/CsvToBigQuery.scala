package dev.rk

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

// Simple csv to BigQuery table converter

// usage

//sbt "runMain dev.rk.CsvToBigQuery
//--project=another-yet-test-project --runner=DataflowRunner
//--input=gs://try-b-1-1/yob1880.csv
//--zone=europe-west3-a
//--output=test_dataset.names_example"

object CsvToBigQuery {
  val log = LoggerFactory.getLogger(getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    log.info(s"Moving csv from: ${args("input")} to BigQuery: ${args("output")}")

    val csv = sc.textFile(args.getOrElse("input", ExampleData.CSV_PATH))
    val rows = csv.map(line => {
      val split = line.split(",")
      TableRow("name" -> split(0), "sex" -> split(1), "num" -> split(2).toInt)
    })

    val schema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("name").setType("STRING"),
        new TableFieldSchema().setName("sex").setType("STRING"),
        new TableFieldSchema().setName("num").setType("INTEGER")
      ).asJava
    )

    rows.saveAsBigQuery(args("output"), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)
    sc.close().waitUntilDone()
  }

}