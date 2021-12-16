package com.eci.anaplan.ins.nonauto.services

import com.eci.anaplan.ins.nonauto.configs.INSNonAutoConfig
import com.eci.common.TimeUtils
import com.eci.common.services.PathFetcher
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.ZonedDateTime
import javax.inject.{Inject, Named, Singleton}

/**
 * Source service to fetch different data frames from S3
 */
// TODO: Update dataFrameSource1 column to be read
@Singleton
class INSNonAutoSource @Inject()(val sparkSession: SparkSession, config: INSNonAutoConfig,
                                 @Named("TENANT_ID") val tenantId: String) extends PathFetcher {

  // TODO: Add all the necessary dataframe source here. Each DataFrame Source will be a folder in S3
  lazy val dataFrameSource1: DataFrame = readByDefaultRange("oracle.exchange_rates")
  lazy val ExchangeRateDf: DataFrame = readByDefaultCustom("oracle.exchange_rates","conversion_date_date")
  lazy val INSNonAutoDf: DataFrame = readByDefaultCustom("insurance.sales_bpng","booking_issued_date_date",true)

  val flattenerSrc: String = config.flattenerSrc
  val flattenerSrcDtl: String = config.flattenerSrcDtl

  // The start date for this aggregation Process
  val utcZonedStartDate: ZonedDateTime = config.utcZonedStartDate

  // The end date for this aggregation
  val utcZonedEndDate: ZonedDateTime = config.utcZonedEndDate

  // To process data up to the passed in end date, we need to query data lake using the required format, because
  // that is how DataLake/Spark partitions the data
  // Aggregator only takes in booking issue date. So to query datalake by created at, we need to plus one month
  // from the passed in end date, due to hanging task
  // TODO: You may want to edit this data according to your report logic
  val endDateToQueryDataLake: String = TimeUtils.utcDateTimeString(utcZonedEndDate.plusDays(1))

  // DataLake stores the data in yyyyMMdd format. To Query DataLake we need to trim out all the time component from the start date
  val startDateToQueryDataLake: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusDays(1))

  // Joined domains such as sales invoice, purchase delivery etc. can be one month before the sales delivery
  // Or to be exactly, one month before the booking issue date
  // TODO: You may want to edit this data according to your report logic
  val startDateToQueryDataLakeForJoinedDomain: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusMonths(1))
}
