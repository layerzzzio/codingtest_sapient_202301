package com.cts

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}

object SparkUtils {
  val spark: SparkSession = SparkSession.builder.master("local").appName("CTS").getOrCreate

  /**
   * Loads CSV data and casts field(s)
   *
   * @return dataframe
   */
  def loadCSV(filepath: String): DataFrame = {
    // load data into dataframe
    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(filepath)

    df.withColumn(
      "timestamp"
      , to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
  }

  /** Returns a dataframe including a sessions id
   *
   * @param df dataframe where add session_id
   * @return a dataframe with a session id per user
   */
  def addSessionId(df: DataFrame): DataFrame = {
    // expiry time
    val expiryTimeInSec = 1800

    // define window and group by userid
    val w = Window.partitionBy("userid").orderBy("timestamp")

    // add session_id
    val dfWithSessionId = df
      .withColumn("_sys_diff", unix_timestamp(col("timestamp")) - unix_timestamp(lag("timestamp", 1).over(w)))
      .withColumn("_sys_seq", row_number().over(w))
      .withColumn(
        "session_id"
        , when(col("_sys_diff") >= 1800 || col("_sys_diff").isNull, col("_sys_seq"))
      )
      .withColumn(
        "session_id"
        , coalesce(col("session_id"), last("session_id", ignoreNulls = true).over(w))
      )
      .withColumn("year", year(col("timestamp")))
      .withColumn("month", month(col("timestamp")))
      .withColumn("day", dayofmonth(col("timestamp")))

    dfWithSessionId.show()

    // filter and return
    val prefix = "_sys"
    val filteredColumns = dfWithSessionId.columns.filter(!_.startsWith(prefix))
    val result = dfWithSessionId.select(filteredColumns.map(col): _*)

    result
  }

  def userSessionTime(df: DataFrame): DataFrame = {
    val wus = Window.partitionBy("userid", "session_id").orderBy("timestamp")
    val wu = Window.partitionBy("userid")

    val result = df
      .withColumn("lag_timestamp", lag(col("timestamp"), 1).over(wus))
      .withColumn(
        "diff"
        , coalesce(unix_timestamp(col("timestamp")) - unix_timestamp(col("lag_timestamp")), lit(0))
      )
      .withColumn(
        "diff"
        , when(col("diff") === lit(0), lit(1800)).otherwise(col("diff"))
      )
      .withColumn("duration", sum(col("diff")).over(wu))
      .select("userid", "duration")
      .distinct

    result
  }
}
