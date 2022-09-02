package com.eci.common.services

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DateType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Date, Timestamp}
import java.time.Duration
import javax.inject.Inject

class DateTimeService @Inject()(sparkSession: SparkSession) extends Serializable  {
  import sparkSession.implicits._
  /**
   * Get array of distinct date from input dataframe and additional timespan to add
   * input : list of non-distinct timestamp (one column, must be with column name = timestamp)
   * timespan : timespan to add to each timestamp before distinct to date
   * timespan_unit : unit of timespan (year, month, week, day, hour, minute, second)
   */
  def getDateArray(input : DataFrame, timespan : Integer, timespan_unit : String): Array[Any] = {
    val timespanExpr = "INTERVAL " + timespan.toString + " " + timespan_unit
    val colName = input.columns(0)

    val minMax = input
      .withColumn(
        "max",
        col(colName) + lit(expr(timespanExpr))
      ).withColumn(
      "min",
      col(colName) - lit(expr(timespanExpr))
    )

    val dateRangeUDF = udf(getAllDateBetween _, ArrayType(DateType))

    val minMaxWithRange = minMax.withColumn(
      "range",
      dateRangeUDF(col("min"), col("max"))
    )

    minMaxWithRange
      .withColumn("date", explode(col("range")))
      .drop("range", "min", "max")
      .select(date_format($"date","yyyyMMdd")).distinct()
      .sort($"date".asc)
      .collect.flatMap(_.toSeq)
  }

  def getAllDateBetween(
                         dateFrom: Timestamp,
                         dateTo: Timestamp
                       ): Seq[Date] = {
    val daysBetween = Duration
      .between(
        dateFrom.toLocalDateTime.toLocalDate.atStartOfDay(),
        dateTo.toLocalDateTime.toLocalDate.atStartOfDay()
      )
      .toDays

    val newRows = Seq.newBuilder[Date]
    // get all intermediate dates
    for (day <- 0L to daysBetween) {
      val date = Date.valueOf(dateFrom.toLocalDateTime.toLocalDate.plusDays(day))
      newRows += date
    }
    newRows.result()
  }
}
