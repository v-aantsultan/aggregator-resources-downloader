package com.eci.anaplan.services

import java.time.ZonedDateTime
import com.eci.common.TimeUtils
import com.eci.anaplan.configs.GVIssuedConfig
import com.eci.common.services.PathFetcher
import javax.inject.{Inject, Named, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Source service to fetch different data frames from S3
 */
@Singleton
class GVIssuedSource @Inject()(val sparkSession: SparkSession,
                               config: GVIssuedConfig,
                               @Named("TENANT_ID") val tenantId: String) extends PathFetcher {

  lazy val dataFrameSource1: DataFrame = readByDefaultRange("oracle.exchange_rates")
  lazy val GVIssuedlistDf: DataFrame = readByDefaultCustom("gift_voucher.gv_issuedlist","issued_date_date")
  lazy val ExchangeRateDf: DataFrame = readByDefaultCustom("oracle.exchange_rates","conversion_date_date")

  val flattenerSrc: String = config.flattenerSrc
  val flattenerSrcDtl: String = config.flattenerSrcDtl
  val flattenerLocal: String = "/Datalake"

  // The start date for this aggregation Process
  val utcZonedStartDate: ZonedDateTime = config.utcZonedStartDate

  // The end date for this aggregation
  val utcZonedEndDate: ZonedDateTime = config.utcZonedEndDate

  // To process data up to the passed in end date, we need to query data lake using the required format, because
  // that is how DataLake/Spark partitions the data
  // Aggregator only takes in booking issue date. So to query datalake by created at, we need to plus one month
  // from the passed in end date, due to hanging task
  val endDateToQueryDataLake: String = TimeUtils.utcDateTimeString(utcZonedEndDate.plusDays(1))

  // DataLake stores the data in yyyyMMdd format. To Query DataLake we need to trim out all the time component from the start date
  val startDateToQueryDataLake: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusDays(1))

  // Joined domains such as sales invoice, purchase delivery etc. can be one month before the sales delivery
  // Or to be exactly, one month before the booking issue date
  val startDateToQueryDataLakeForJoinedDomain: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusMonths(1))
}
