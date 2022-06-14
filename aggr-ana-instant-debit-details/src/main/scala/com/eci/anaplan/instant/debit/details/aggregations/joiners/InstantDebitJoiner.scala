package com.eci.anaplan.instant.debit.details.aggregations.joiners

import com.eci.anaplan.instant.debit.details.aggregations.constructors.{AssignedPaymentDf, ExchangeRateDf, MDRChargesDf, PaymentScopeSheetDf}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, count, lit, substring, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class InstantDebitJoiner @Inject()(spark: SparkSession,
                                   AssignedPaymentDf: AssignedPaymentDf,
                                   PaymentScopeSheetDf: PaymentScopeSheetDf,
                                   ExchangeRateDf: ExchangeRateDf,
                                   MDRChargesDf: MDRChargesDf) {

  def get: DataFrame = {

    import spark.implicits._

    val ASP = AssignedPaymentDf.get
    val PS = PaymentScopeSheetDf.get
    val RATE = ExchangeRateDf.get
    val MDR = MDRChargesDf.get

    val JoinPaymentScopeMapping = ASP
      .join(PS,
        ASP("payment_scope") === PS("instant_debit_payment_scope"),
        "left"
      )
      .withColumn("flag",coalesce(PS("instant_debit_payment_scope"),lit("exclude")))

    val FilterByPaymentScope = JoinPaymentScopeMapping.filter($"flag" =!= "exclude")

    val JoinedMDR = FilterByPaymentScope
      .join(MDR,
        MDR("booking_id") === FilterByPaymentScope("booking_id"),
        "left"
      )
      .withColumn("count_bid",
        count(FilterByPaymentScope("booking_id")).over(Window.partitionBy(FilterByPaymentScope("booking_id")))
      )
      .withColumn("sum_payment_amount_bid",
        sum(FilterByPaymentScope("payment_amount")).over(Window.partitionBy(FilterByPaymentScope("booking_id")))
      )
      .withColumn("mdr_charges",
        (($"sum_payment_amount_bid" / MDR("expected_amount")) * MDR("mdr_amount")) / $"count_bid"
      )
      .drop(MDR("booking_id"))

    val JoinedRate = JoinedMDR
      .join(RATE,
        JoinedMDR("payment_currency") === RATE("from_currency")  &&
          JoinedMDR("issued_time_formatted") === RATE("conversion_date"),
        "left"
      )
      .withColumn("conversion_rate_formatted",
        when(JoinedMDR("payment_currency") === "IDR",lit(1))
          .otherwise(RATE("conversion_rate"))
      )

    val Final = JoinedRate
      .select(
        $"issued_time_formatted",
        substring($"product_type",1,2).as("product_type"),
        $"payment_currency",
        $"payment_scope",
        $"payment_id",
        coalesce($"payment_amount" * $"conversion_rate_formatted",lit(0)).as("payment_amount_idr"),
        coalesce($"mdr_charges" * $"conversion_rate_formatted",lit(0)).as("mdr_charges_idr"),
        lit(0).as("point_grant_idr")
      )

    Final

  }
}
