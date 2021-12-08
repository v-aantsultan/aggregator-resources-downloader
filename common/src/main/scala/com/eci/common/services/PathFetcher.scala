package com.eci.common.services

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Common Trait to fetch path for source
 */
trait PathFetcher {
  val sparkSession: SparkSession

  import sparkSession.implicits._

  val flattenerSrc: String
  val flattenerSrcDtl: String

  val tenantId: String

  // The start date to query data lake for most domains
  val startDateToQueryDataLake: String

  // The start date to query data lake for joined domains
  val startDateToQueryDataLakeForJoinedDomain: String

  // The end date to query data lake
  val endDateToQueryDataLake: String

  def readByCustomRange(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"conversion_date_date" >= startDate && $"conversion_date_date" <= endDate)
  }

  def readByCustom(domain: String, startDate: String, endDate: String, ColumnKey: String, MergeSchema: Boolean): DataFrame = {
    if (domain.contains("eci_sheets/")) {
      sparkSession.read
        .parquet(s"$flattenerSrcDtl/$domain")
    } else if (MergeSchema) {
      sparkSession.read
        .option("mergeSchema", "true")
        .parquet(s"$flattenerSrc/$domain")
        .filter(col(ColumnKey) >= startDate && col(ColumnKey) <= endDate)
    } else {
      sparkSession.read
        .parquet(s"$flattenerSrc/$domain")
        .filter(col(ColumnKey) >= startDate && col(ColumnKey) <= endDate)
    }
  }

  def readByDefaultRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultCustom(domain: String, ColumnKey: String = "", MergeSchema: Boolean = false): DataFrame = {
    readByCustom(domain, startDateToQueryDataLake, endDateToQueryDataLake, ColumnKey, MergeSchema)
  }

  def readByJoinedDomainRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLakeForJoinedDomain, endDateToQueryDataLake)
  }
}