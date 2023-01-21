package com.cts

import com.cts.SparkUtils.{addSessionId, loadCSV, userSessionTime}
import com.cts.Utils.{dropDuplicates, readCSV}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, _}

import java.nio.file.Paths

object Main extends App {
  val PATH = Paths.get("src/main/scala/com/cts").toAbsolutePath

  // ---------------------------
  // QUESTION 1
  // ---------------------------
  // 1. read CSV
  private val rows = readCSV(s"$PATH/1_userlocation.csv")

  // 2. deduplicate
  private val rowsDeduped = dropDuplicates(rows, Seq(("name", StringContext), ("age", Int)))
  println(rowsDeduped)

  // ---------------------------
  // QUESTION 2
  // ---------------------------
  // 1. load data
  private val raw = loadCSV(s"$PATH/2_session.csv")

  // 2. add session id
  private val allSessions = addSessionId(raw)
  allSessions.show()

  // 3. write to parquet and partitionBy year, month & day
  allSessions
    .write
    .partitionBy("year", "month", "day")
    .mode("overwrite")
    .parquet(s"$PATH/data/q2_sessions_id.parquet")

  // ---------------------------
  // QUESTION 3
  // ---------------------------
  // load for q3.1 and q3.2
  private val dailySessions = SparkUtils.spark.read
    .parquet(s"$PATH/data/q2_sessions_id.parquet")
    .filter("year = '2018' AND month = '1' AND day = '1'")
  private val dsCached = dailySessions.coalesce(1000).cache()

  // q3.1. daily sessions
  private val dailySessionCnt = dsCached.agg(countDistinct("session_id", "userid").alias("daily_session_cnt"))
  dailySessionCnt.show()

  // q3.2. total time spent by a user in a day
  private val dsU1= dsCached.filter(col("userid") === lit("u1"))
  private val dailyUserSessionTime = userSessionTime(dsU1)
  dailyUserSessionTime.show()

  dsCached.unpersist()

  // q3.3. total time spent by a user over a month
  private val monthlySessions = SparkUtils.spark.read
    .parquet(s"$PATH/data/q2_sessions_id.parquet")
    .filter("year = '2018' AND month = '1' AND userid = 'u1'")

  private val monthlyUserSessionTime = userSessionTime(monthlySessions)
  monthlyUserSessionTime.show()
}