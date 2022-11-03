package com.eci.anaplan.ci.details.aggregations.joiners

import com.eci.anaplan.ci.details.aggregations.constructors.ConnectivityInternationalDf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class ConnectivityInternationalDetails @Inject()(spark: SparkSession,
                                                 connectivityDomesticDf: ConnectivityInternationalDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    connectivityDomesticDf.get
      .groupBy(
        $"report_date", $"customer", $"business_model", $"business_partner", $"product_category", $"payment_channel"
      )
      .agg(
        coalesce(countDistinct($"booking_id"), lit(0)).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"), lit(0)).cast(IntegerType).as("no_of_coupon"),
        coalesce(sum($"quantity"), lit(0)).cast(IntegerType).as("transaction_volume"),
        coalesce(sum($"total_published_rate_idr"), lit(0)).cast(DecimalType(18,4)).as("gmv"),
        coalesce(sum(
          when(upper($"business_model") === "CONSIGNMENT", lit(0))
            .otherwise($"total_published_rate_idr")
        ), lit(0)).cast(DecimalType(18,4)).as("gross_revenue"),
        coalesce(sum(
          when(upper($"business_model") === "CONSIGNMENT", $"gross_commission_idr")
            .otherwise(0)
        ), lit(0)).cast(DecimalType(18,4)).as("commission"),
        coalesce(
          sum(
            when($"discount_or_premium_idr".lt(0), $"discount_or_premium_idr" + $"discount_wht_collect_to_idr")
              .otherwise(lit(0))
          ),
          lit(0)).cast(DecimalType(18,4)).as("discount"),
        coalesce(
          sum(
            when($"discount_or_premium_idr".geq(0), $"discount_or_premium_idr")
              .otherwise(lit(0))
          ),
          lit(0)).cast(DecimalType(18,4)).as("premium"),
        coalesce(sum($"unique_code_idr"), lit(0)).cast(DecimalType(18,4)).as("unique_code"),
        coalesce(sum($"coupon_value_idr"), lit(0)).cast(DecimalType(18,4)).as("coupon"),
        coalesce(
          when(upper($"business_model") === "CONSIGNMENT", lit(0))
            .otherwise(sum($"nta_idr" * -1))
        , lit(0)).cast(DecimalType(18,4)).as("nta"),
        lit(0).as("transaction_fee"),
        coalesce(
          sum($"vat_out_idr" * -1), lit(0)
        ).cast(DecimalType(18,4)).as("vat_out"),
        coalesce(sum($"point_redemption_idr"), lit(0)).cast(DecimalType(18,4)).as("point_redemption"),
        coalesce(sum($"mdr_charges_idr" * -1), lit(0)).cast(DecimalType(18,4)).as("mdr_charges")
      )
      .select($"*")
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}