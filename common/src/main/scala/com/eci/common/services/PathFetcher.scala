package com.eci.common.services

import com.eci.common.TimeUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.ZonedDateTime

/**
 * Common Trait to fetch path for source
 */
trait PathFetcher {
  val sparkSession: SparkSession

  import sparkSession.implicits._

  val flattenerSrc: String
  val flattenerSrcDtl: String
  val flattenerLocal: String

  val tenantId: String

  // The start date to query data lake for most domains
  val startDateToQueryDataLake: String
  val utcZonedStartDate: ZonedDateTime

  // The start date to query data lake for joined domains
  val startDateToQueryDataLakeForJoinedDomain: String

  // The end date to query data lake
  val endDateToQueryDataLake: String
  val utcZonedEndDate: ZonedDateTime

  def readByCustomRange(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"conversion_date_date" >= startDate && $"conversion_date_date" <= endDate)
  }

  def readByCustom(domain: String, startDate: String, endDate: String, ColumnKey: String, MergeSchema: Boolean): DataFrame = {
    if (MergeSchema) {
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

  def readByCustomDtl(domain: String, startDate: String, endDate: String, ColumnKey: String): DataFrame = {
    if (domain.contains("eci_sheets/") || domain.contains("business_platform_master/")) {
      sparkSession.read
        .parquet(s"$flattenerSrcDtl/$domain")
    } else if (domain.contains("business_platform/")) {
      sparkSession.read
        .parquet(s"$flattenerSrcDtl/$domain")
        .filter(col(ColumnKey) >= startDate && col(ColumnKey) <= endDate && $"tenant_id" === tenantId)
    } else {
      sparkSession.read
        .parquet(s"$flattenerSrcDtl/$domain")
        .filter(col(ColumnKey) >= startDate && col(ColumnKey) <= endDate)
    }
  }

  def readByCustomLocal(domain: String, startDate: String, endDate: String, ColumnKey: String): DataFrame = {
    if (domain.contains("eci_sheets/") || domain.contains("business_platform_master/")) {
      sparkSession.read
        .parquet(s"$flattenerLocal/$domain")
    } else if (domain.contains("business_platform/")) {
      sparkSession.read
        .parquet(s"$flattenerLocal/$domain")
        .filter(col(ColumnKey) >= startDate && col(ColumnKey) <= endDate && $"tenant_id" === tenantId)
    } else {
      sparkSession.read
        .parquet(s"$flattenerLocal/$domain")
        .filter(col(ColumnKey) >= startDate && col(ColumnKey) <= endDate)
    }
  }

  def readByDefaultRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultCustom(domain: String, ColumnKey: String, MergeSchema: Boolean = false, Duration: Int = 1): DataFrame = {
    val startDate: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusDays(Duration))
    val endDate: String = TimeUtils.utcDateTimeString(utcZonedEndDate.plusDays(Duration))
    readByCustom(domain, startDate, endDate, ColumnKey, MergeSchema)
  }

  def readByDefaultCustomDtl(domain: String, ColumnKey: String = "", Duration: Int = 1): DataFrame = {
    val startDate: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusDays(Duration))
    val endDate: String = TimeUtils.utcDateTimeString(utcZonedEndDate.plusDays(Duration))
    readByCustomDtl(domain, startDate, endDate, ColumnKey)
  }

  def readByDefaultLocal(domain: String, ColumnKey: String = "", Duration: Int = 1): DataFrame = {
    val startDate: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusDays(Duration))
    val endDate: String = TimeUtils.utcDateTimeString(utcZonedEndDate.plusDays(Duration))
    readByCustomLocal(domain, startDate, endDate, ColumnKey)
  }

  def readByJoinedDomainRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLakeForJoinedDomain, endDateToQueryDataLake)
  }
}