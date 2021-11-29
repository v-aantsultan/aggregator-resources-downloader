package com.eci.anaplan.aggregations.constructors

// import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

/**
  * DataFrame constructor for Sales Invoice
  *
  * @param sparkSession    The spark session
  * // @param s3SourceService Service with all S3 data frames
  */
// TODO: Update TestDataFrame1 and queries required
@Singleton
class GrandProductTypeDf @Inject()(val sparkSession: SparkSession) {

  import sparkSession.implicits._
  import GrandProductTypeDf._

  def get: DataFrame = {

    // TODO : Update this part of the code to get Domain data from S3
    // s3SourceService.GrandProductTypeDf
    Seq(
      ("HOTEL_TOMANG_COMPLIMENT_NEW", "Complimentary Points"),
      ("MARKETING_ONLINE_ATTRACTIONS_NEW", "Ops & Marketing Points"),
      ("MARKETING_OFFLINE_ATTRACTIONS_NEW", "Ops & Marketing Points"),
      ("OPERATION_NEW", "Ops & Marketing Points"),
      ("FLIGHT_NEW", "Flight"),
      ("HOTEL_NEW", "Hotel"),
      ("SELLING_POINTS_INVOICING", "Selling Points"),
      ("SELLING_POINTS_NEW", "Selling Points"),
      ("SELLING_POINTS", "Selling Points"),
      ("SELLING_POINTS_VOUCHER_CODE", "Selling Points"),
      ("MISSION_CENTRAL_NEW", "Employee Reward Points"),
      ("TVALLOW_NEW", "Employee Reward Points"),
      ("TVLALLOW_PRODDEVELOP_NEW", "Employee Reward Points"),
      ("TVLALLOW_GA_NEW", "Employee Reward Points"),
      ("TVLALLOW_PRODSOURCING_NEW", "Employee Reward Points"),
      ("TVLALLOW_ADV_AND_PROMOTION_NEW", "Employee Reward Points")
    ).toDF(map_cost_type_id, map_grant_product_type)
  }
}

object GrandProductTypeDf {
  private val map_cost_type_id = "map_cost_type_id"
  private val map_grant_product_type = "map_grant_product_type"
}