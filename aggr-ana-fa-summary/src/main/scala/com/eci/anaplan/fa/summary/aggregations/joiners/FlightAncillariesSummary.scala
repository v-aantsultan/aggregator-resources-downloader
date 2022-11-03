package com.eci.anaplan.fa.summary.aggregations.joiners

import com.eci.anaplan.fa.summary.aggregations.constructors.FlightAncillariesSummaryDf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightAncillariesSummary @Inject()(spark: SparkSession, FlightAncillariesSummaryDf: FlightAncillariesSummaryDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val FlightAncillariesSummaryFinal = FlightAncillariesSummaryDf.get
      .groupBy($"report_date", $"customer", $"business_model", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).cast(IntegerType).as("no_of_coupon")
      )
      .select(
        $"*"
      )
    FlightAncillariesSummaryFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
