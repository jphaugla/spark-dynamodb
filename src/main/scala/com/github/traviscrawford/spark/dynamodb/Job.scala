package com.github.traviscrawford.spark.dynamodb

import com.twitter.app.FlagParseException
import com.twitter.app.FlagUsageError
import com.twitter.app.Flags
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** Base class for Spark jobs. */
trait Job {
  protected val log = LoggerFactory.getLogger(this.getClass)
  protected val name: String = getClass.getName.stripSuffix("$")

  protected val flag: Flags =
    new Flags(this.getClass.getName, includeGlobal = true, failFastUntilParsed = true)

// Create a SparkSession. No need to create SparkContext
// You automatically get it as part of the SparkSession
  lazy implicit val spark = SparkSession
   .builder()
   .appName(getClass.getName)
   .enableHiveSupport()
   .getOrCreate()
  /** Users should override this method with their Spark job logic. */
  def run(): Unit

  def main(args: Array[String]): Unit = {
    flag.parseArgs(args, allowUndefinedFlags = false) match {
      case Flags.Ok(remainder) =>
      case Flags.Help(usage) => throw FlagUsageError(usage)
      case Flags.Error(reason) => throw FlagParseException(reason)
    }

    try {
      run()
    } finally {
      spark.stop()
    }
  }
}
