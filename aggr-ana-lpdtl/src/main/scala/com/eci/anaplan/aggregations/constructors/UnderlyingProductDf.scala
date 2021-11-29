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
class UnderlyingProductDf @Inject()(val sparkSession: SparkSession) {

  import sparkSession.implicits._
  import UnderlyingProductDf._

  def get: DataFrame = {

    // TODO : Update this part of the code to get Domain data from S3
    // s3SourceService.GrandProductTypeDf
    Seq(
      ("HT","HT"),
      ("FL","FL"),
      ("EX","AA"),
      ("BS","BS"),
      ("CU","CU"),
      ("LP","LP"),
      ("BD","FBHB"),
      ("TR","TR"),
      ("CR","CR"),
      ("AT","AT")
    ).toDF(map_booking_product_type, map_underlying_product)
  }
}

object UnderlyingProductDf {
  private val map_booking_product_type = "map_booking_product_type"
  private val map_underlying_product = "map_underlying_product"
}