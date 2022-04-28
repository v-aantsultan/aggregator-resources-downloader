package com.eci.anaplan.cr.aggr.aggregations.joiners

import com.eci.anaplan.cr.aggr.aggregations.constructors.{CouponReportIDRDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import javax.inject.{Inject, Singleton}

@Singleton
class CouponReportAggr @Inject()(spark: SparkSession,
                                 couponReportIDRDf: CouponReportIDRDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    couponReportIDRDf.get
      .groupBy($"report_date", $"coupon_issuer", $"product", $"customer")
      .agg(
        coalesce(countDistinct($"coupon_booking_concate")).as("no_of_coupon"),
        coalesce(sum($"coupon_allocated_amount_idr"/ $"count_coupon_booking_concate")).as("coupon")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}