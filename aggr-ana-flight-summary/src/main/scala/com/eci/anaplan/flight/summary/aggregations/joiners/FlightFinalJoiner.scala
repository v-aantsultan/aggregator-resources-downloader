package com.eci.anaplan.flight.summary.aggregations.joiners

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import javax.inject.{Inject, Singleton}

@Singleton
class FlightFinalJoiner @Inject()(
                             spark: SparkSession,
                             flightFlattenerJoiner: FlightFlattenerJoiner
                           ) {

  import spark.implicits._

  def get: DataFrame = {
    flightFlattenerJoiner.get
      .groupBy(
        $"report_date", $"customer", $"business_model", $"product", $"product_category", $"payment_channel"
      )
      .agg(
        coalesce(countDistinct($"booking_id"), lit(0)).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"), lit(0)).as("no_of_coupon")
      )
      .select($"*")
  }
}