package com.github.traviscrawford.spark.dynamodb
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.driver.core.exceptions.AlreadyExistsException


import java.net.URI

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal



/** Backup a DynamoDB table as JSON.
  *
  * The full table is scanned and the results are stored in the given output path.
  */
object DynamoGetRDD extends Job {
  private val region = flag[String]("region", "Region of the DynamoDB table to scan.")
  private val endpoint = flag[String]("endpoint", "Endpoint of the DynamoDB table to scan.")

  private val tableName = flag[String]("table", "DynamoDB table to scan.")

  private val totalSegments = flag("totalSegments", "1",
    "Number of DynamoDB parallel scan segments.")

  private val pageSize = flag("pageSize", "1000", "Page size of each DynamoDB request.")

  private val output = flag[String]("output", "Path to write the DynamoDB table backup.")

  private val overwrite = flag("overwrite", false, "Set to true to overwrite output path.")

  private val credentials = flag[String]("credentials",
    "Optional AWS credentials provider class name.")

  private val rateLimit = flag[Int]("rateLimit",
    "Max number of read capacity units per second each scan segment will consume.")

  def getKeys(table_name: String): List[String] = {
    log.info("Getting description for %s\n\n", table_name)
    val ddb = AmazonDynamoDBClientBuilder.defaultClient
    var keyList = new ListBuffer[String]()
    try {
      val table_info = ddb.describeTable(table_name).getTable
      val keyschema = table_info.getKeySchema()
      log.info("Keys")
      import scala.collection.JavaConversions._
      for (k <- keyschema) {
        log.info(k.getAttributeName + "(" + k.getKeyType + ")\n")
        keyList += k.getAttributeName
      }

    } catch {
      case NonFatal(err) =>
        log.error(s"Failed getting key table information for: ${table_name}", err)
    }
    log.info("\nDone!")
    (keyList.toList)
  }
/*
 def getSchema (table_name: String): TableSchema = {
    val scanSpec = new ScanSpec().withMaxPageSize(pageSize)
    val result = Table.scan(scanSpec)
    val json = result.firstPage().iterator().map(_.toJSON)
    import sqlContext.implicits._  // scalastyle:ignore
    val jsonDS = sqlContext.sparkContext.parallelize(json.toSeq).toDS()
    val jsonDF = sqlContext.read.json(jsonDS)
    jsonDF.schema
  }
*/
  def run(): Unit = {
    val maybeCredentials = if (credentials.isDefined) Some(credentials()) else None
    val maybeTableName = if (tableName.isDefined) Some(tableName()) else None
    val maybeRateLimit = if (rateLimit.isDefined) Some(rateLimit()) else None
    val maybeRegion = if (region.isDefined) Some(region()) else None
    val maybeTotalSegments = if (totalSegments.isDefined) Some(totalSegments()) else None
    val maybePageSize = if (pageSize.isDefined) Some(pageSize()) else None
    val maybeFilterExpression =  None
    val schemais =
      StructType(
        Array(
          StructField("hash_key", LongType),
          StructField("city", StringType),
          StructField("sort_key", LongType),
          StructField("puppy_count", LongType)
        )
      )
    val maybeSchema =  Some(schemais)
    val awsAccessKey = None
    val awsSecretKey = None
    val maybeEndpoint = if (endpoint.isDefined) Some(endpoint()) else None
    val returnRDD = DynamoScannerRDD(spark, tableName(), maybeFilterExpression, maybePageSize, maybeTotalSegments, maybeRateLimit,
      maybeRegion, maybeSchema, maybeCredentials, awsAccessKey, awsSecretKey,   maybeEndpoint)
    import spark.implicits._
    val returnDF = spark.createDataFrame(returnRDD,schemais)
    returnDF.printSchema()
    returnDF.show(5)
    val keycols = getKeys(tableName())
    log.info(s"print key columns")
    keycols.foreach {println}
    //  hash_key is always first
    // initialize sort key as it may be null
    var sort_key = "na"
    if (keycols.length > 1) sort_key = keycols(1).toLowerCase
    val hash_key = keycols(0).toLowerCase()

    //  gets all columns labels into a list, this will be used for list of json columns
    val cols = returnDF.columns.toSeq
    // remove the hash_key and the sort_key as they should not be in json string
    val othercols = cols.filterNot(keycols.toSet)
    // val othercols = cols.filterNot(x => x == hash_key).filterNot(x => x == sort_key)
    println(s"print columns for cols")
    cols.foreach {println}
    println(s"print columns for othercols")
    othercols.foreach {println}
    //  create string to be used within the expression to add the structype column
    val expressString = "(" + othercols.mkString(",") + ")"
    val newDF = returnDF.withColumn("structure",expr(expressString))
      .withColumn("json_blob", expr("to_json(structure)"))
    //  this show causes breakage on long to string conversion
    newDF.show(2)
    newDF.printSchema()
    //  Only need to write out the three columns
    var writeDF  = spark.emptyDataFrame
    if (keycols.length > 1) {
      // writeDF = newDF.select(col(hash_key).cast, col(sort_key), col("json_blob"))
      writeDF = newDF.select(col(hash_key), col(sort_key), col("json_blob"))
    } else {
      writeDF = newDF.select(col(hash_key),  col("json_blob"))
    }
    writeDF.printSchema()
    log.info(s"before create cassandra table " + tableName() + hash_key + sort_key)
    try {
      if (keycols.length > 1) {
        writeDF.createCassandraTable("testks",tableName().toLowerCase(),partitionKeyColumns = Some(Seq(hash_key))
          ,clusteringKeyColumns = Some(Seq(sort_key)))
      } else {
        writeDF.createCassandraTable("testks",tableName().toLowerCase(),partitionKeyColumns = Some(Seq(hash_key))
        )
      }
    } catch {
      case ex: AlreadyExistsException => log.info(tableName() + " already existed so did not recreate");
      case ex: Exception => ex.printStackTrace();
    }
    log.info(s"before write cassandra " +  tableName() +  hash_key + sort_key)
    try {
      writeDF.write.cassandraFormat(tableName().toLowerCase, "testks").mode(SaveMode.Append).save()
    } catch {
      case ex: Exception =>
        log.error("Error in write to " + tableName())
        ex.printStackTrace()
    }
    log.info(s"after write cassandra, " +  tableName() +  hash_key + sort_key)
  }

}
