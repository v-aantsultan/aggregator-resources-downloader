package com.eci.anaplan.bs.summary.aggregations.joiners

import com.eci.anaplan.bs.summary.aggregations.constructors.BusSummaryDf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BusSummary @Inject()(spark: SparkSession,
                           BusSummaryDf: BusSummaryDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val BusSummaryFinal = BusSummaryDf.get
      .groupBy($"report_date",
        $"customer",
        $"business_model",
        $"product",
        coalesce($"payment_channel",lit("None")).as("payment_channel"))
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).as("no_of_coupon")
      )
      .select(
        $"*"
      )
    BusSummaryFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
