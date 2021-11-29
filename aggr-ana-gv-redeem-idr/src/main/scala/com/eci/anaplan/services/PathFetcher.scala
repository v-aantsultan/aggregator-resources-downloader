package com.eci.anaplan.services

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

  def readWithoutDateDWH(domain: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrcDtl/$domain")
  }

  def readByConversionDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"conversion_date_date" >= startDate && $"conversion_date_date" <= endDate)
  }

  def readByRedemptionDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"redemption_date_date" >= startDate && $"redemption_date_date" <= endDate)
  }

  def readByDefaultRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readDefaultWithoutDateDWH(domain: String): DataFrame = {
    readWithoutDateDWH(domain)
  }

  def readByDefaultConversionDateDWH(domain: String): DataFrame = {
    readByConversionDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultRedemptionDateDWH(domain: String): DataFrame = {
    readByRedemptionDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByJoinedDomainRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLakeForJoinedDomain, endDateToQueryDataLake)
  }
}
