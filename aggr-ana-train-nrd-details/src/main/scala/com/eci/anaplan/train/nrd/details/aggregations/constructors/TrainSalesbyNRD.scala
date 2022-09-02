package com.eci.anaplan.train.nrd.details.aggregations.constructors

import com.eci.common.master.datawarehouse.TrainSalesDf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.eci.common.constant.Constant
import org.apache.spark.sql.functions.{expr, lit, size, split, to_date, when}

import javax.inject.{Inject, Singleton}

@Singleton
class TrainSalesbyNRD @Inject()(val sparkSession: SparkSession, TrainSalesDf: TrainSalesDf) {

  import sparkSession.implicits._

  def get: DataFrame = {

    val BusinessModelIsCommission = $"business_model" === "COMMISSION"

    val TrainbyNRD = TrainSalesDf.getallperiod
      .filter($"`final_refund_status`" === "Not Refunded")

    TrainbyNRD
      .withColumn("coupon_code_total",
        size(split($"`coupon_code`",",")))

      .select(
        to_date($"non_refundable_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        split($"`locale`","_")(1).as("customer"),
        when(BusinessModelIsCommission, lit("Consignment"))
          .otherwise("business_model")
          .as("business_model"),
        $"fulfillment_id".as("business_partner"),
        when($"fulfillment_id" === "KAI_trinusa", lit("KAI Ticket"))
          .otherwise(lit("Global Train"))
          .as("product_category"),
        $"pd_booking_id".as("no_of_transactions"),
        when($"coupon_code".isNull || $"coupon_code".isin("N/A","","blank"), Constant.LitZero)
          .otherwise($"coupon_code_total")
          .as("no_of_coupon"),
        $"pax_quantity".as("transaction_volume"),
        $"published_rate_contracting_idr".as("gmv"),
        when(BusinessModelIsCommission, Constant.LitZero)
          .otherwise($"published_rate_contracting_idr")
          .as("gross_revenue"),
        when(BusinessModelIsCommission, Constant.LitZero)
          .otherwise($"provider_commission_idr")
          .as("commission"),
        when($"discount_or_premium_idr" < 0, $"discount_or_premium_idr" + $"wht_discount_idr")
          .otherwise(Constant.LitZero)
          .as("discount"),
        when($"discount_or_premium_idr" >= 0, $"discount_or_premium_idr")
          .otherwise(Constant.LitZero)
          .as("premium"),
        $"unique_code_idr".as("unique_code"),
        $"coupon_amount_idr".as("coupon"),
        when(BusinessModelIsCommission, Constant.LitZero)
          .otherwise($"total_amount_idr" * lit(-1))
          .as("nta"),
        $"transaction_fee_idr".as("transaction_fee"),
        ($"vat_out_idr" * lit(-1)).as("vat_out"),
        $"point_redemption_idr".as("point_redemption")
      )
  }
}
