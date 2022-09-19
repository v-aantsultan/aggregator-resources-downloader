package com.eci.common.master.datawarehouse

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class TrainSalesDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def getallperiod: DataFrame = {
    s3SourceService.TrainSalesAllPeriodSrc
      .select(
        $"`non_refundable_date`",
        $"`locale`",
        $"`business_model`",
        $"`fulfillment_id`",
        $"`pd_booking_id`",
        $"`coupon_code`",
        $"`pax_quantity`",
        $"`published_rate_contracting_idr`",
        $"`provider_commission_idr`",
        $"`wht_discount_idr`",
        $"`discount_or_premium_idr`",
        $"`unique_code_idr`",
        $"`coupon_amount_idr`",
        $"`total_amount_idr`",
        $"`transaction_fee_idr`",
        $"`vat_out_idr`",
        $"`point_redemption_idr`",
        $"`final_refund_status`",
        $"`delivery_fee_idr`"
      )
  }
}