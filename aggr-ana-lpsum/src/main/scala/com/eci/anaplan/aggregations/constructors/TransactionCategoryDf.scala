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
class TransactionCategoryDf @Inject()(val sparkSession: SparkSession) {

  import sparkSession.implicits._
  import TransactionCategoryDf._

  def get: DataFrame = {

    // TODO : Update this part of the code to get Domain data from S3
    // s3SourceService.GrandProductTypeDf
    Seq(
      ("CANCELLATION", "GRANT"),
      ("EXPIRATION", "EXPIRATION"),
      ("FULL REFUND", "REDEEM"),
      ("GRANT", "GRANT"),
      ("REDEEM", "REDEEM"),
      ("REFUND", "REDEEM"),
      ("REFUND BEFORE ISSUANCE", "REDEEM"),
      ("UNISSUED", "REDEEM"),
      ("UNISSUED NO REFUND", "REDEEM"),
      ("UNISSUED WITH REFUND", "REDEEM")
    ).toDF(map_transaction_type, map_transaction_category)
  }
}

object TransactionCategoryDf {
  private val map_transaction_type = "map_transaction_type"
  private val map_transaction_category = "map_transaction_category"
}