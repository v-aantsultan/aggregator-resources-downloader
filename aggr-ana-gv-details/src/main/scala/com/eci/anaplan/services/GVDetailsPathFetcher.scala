package com.eci.anaplan.services

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Common Trait to fetch path for source
 */
trait GVDetailsPathFetcher {
  val sparkSession: SparkSession

  import sparkSession.implicits._

  val flattenerSrc: String
  val flattenerSrcDtl: String
  val flattenerSrcLocal: String

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

  def readWithoutDateDTL(domain: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrcDtl/$domain")
  }

  def readByRedemptionDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"redemption_date_date" >= startDate && $"redemption_date_date" <= endDate)
  }

  def readByRevenueDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"revenue_date_date" >= startDate && $"revenue_date_date" <= endDate)
  }

  def readByIssuedDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"issued_date_date" >= startDate && $"issued_date_date" <= endDate)
  }

  def readByIssuedDateLocal(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrcLocal/$domain")
      .filter($"issued_date_date" >= startDate && $"issued_date_date" <= endDate)
  }

  def readByReportDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"report_date_date" >= startDate && $"report_date_date" <= endDate)
  }

  def readByConversionDateDWH(domain: String, startDate: String, endDate: String): DataFrame = {
    sparkSession.read
      .parquet(s"$flattenerSrc/$domain")
      .filter($"conversion_date_date" >= startDate && $"conversion_date_date" <= endDate)
  }

  def readByDefaultRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readDefaultWithoutDateDTL(domain: String): DataFrame = {
    readWithoutDateDTL(domain)
  }

  def readByDefaultRedemptionDateDWH(domain: String): DataFrame = {
    readByRedemptionDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultRevenueDateDWH(domain: String): DataFrame = {
    readByRevenueDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultIssuedDateDWH(domain: String): DataFrame = {
    readByIssuedDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultIssuedDateLocal(domain: String): DataFrame = {
    readByIssuedDateLocal(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultReportDateDWH(domain: String): DataFrame = {
    readByReportDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByDefaultConversionDateDWH(domain: String): DataFrame = {
    readByConversionDateDWH(domain, startDateToQueryDataLake, endDateToQueryDataLake)
  }

  def readByJoinedDomainRange(domain: String): DataFrame = {
    readByCustomRange(domain, startDateToQueryDataLakeForJoinedDomain, endDateToQueryDataLake)
  }
}
