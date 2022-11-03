package com.eci.anaplan.ins.details.aggregations.joiners

import com.eci.anaplan.ins.details.aggregations.constructors.{CreditLifeInsuranceDf, INSAutoDf, INSNonAutoDf}
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, sum, when}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class InsuranceDetails @Inject()(spark: SparkSession,
                                 INSAutoDf: INSAutoDf,
                                 INSNonAutoDf: INSNonAutoDf,
                                 CreditLifeInsuranceDf: CreditLifeInsuranceDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val INSAuto = INSAutoDf.get
      .groupBy($"report_date", $"customer", $"business_partner", $"product", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(countDistinct($"policy_id"),lit(0)).as("no_of_policy"),
        coalesce(sum($"num_of_coverage"),lit(0)).as("no_of_insurance_coverage"),
        coalesce(sum($"total_fare_from_provider_idr"),lit(0)).as("gross_written_premium"),
        coalesce(sum($"insurance_commission_idr" + $"total_other_income_idr"),lit(0)).as("commission"),
        coalesce(sum(
          when($"discount_or_premium_idr" < 0,$"discount_or_premium_idr" + $"discount_wht_expense_idr")
            .otherwise(lit(0))
        ),lit(0)).as("discount"),
        coalesce(sum(
          when($"discount_or_premium_idr" >= 0,$"discount_or_premium_idr")
            .otherwise(lit(0))
        ),lit(0)).as("premium"),
        coalesce(sum($"unique_code_idr"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value_idr"),lit(0)).as("coupon"),
        coalesce(sum($"mdr_amount_idr" * -1),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    val INSNonAuto = INSNonAutoDf.get
      .groupBy($"report_date", $"customer", $"business_partner", $"product", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(countDistinct($"policy_id"),lit(0)).as("no_of_policy"),
        coalesce(sum($"num_of_coverage"),lit(0)).as("no_of_insurance_coverage"),
        coalesce(sum($"total_fare_from_provider_idr"),lit(0)).as("gross_written_premium"),
        coalesce(sum($"insurance_commission_idr" + $"total_other_income_idr"),lit(0)).as("commission"),
        coalesce(sum(
          when($"discount_or_premium_idr" < 0,$"discount_or_premium_idr" + $"discount_wht_expense_idr")
            .otherwise(lit(0))
        ),lit(0)).as("discount"),
        coalesce(sum(
          when($"discount_or_premium_idr" >= 0,$"discount_or_premium_idr")
            .otherwise(lit(0))
        ),lit(0)).as("premium"),
        coalesce(sum($"unique_code_idr"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value_idr"),lit(0)).as("coupon"),
        coalesce(sum($"mdr_amount_idr" * -1),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    val CreditLifeInsurance = CreditLifeInsuranceDf.get
      .groupBy($"report_date", $"customer", $"business_partner", $"product", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"loan_id"),lit(0)).as("no_of_transactions"),
        coalesce(countDistinct($"loan_id"),lit(0)).as("no_of_policy"),
        coalesce(countDistinct($"loan_id"),lit(0)).as("no_of_insurance_coverage"),
        coalesce(sum($"insurance_premium"),lit(0)).as("gross_written_premium"),
        coalesce(sum($"referral_fee_amount"),lit(0)).as("commission"),
        coalesce(sum($"discount"),lit(0)).as("discount"),
        coalesce(sum($"premium"),lit(0)).as("premium"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon"),lit(0)).as("coupon"),
        coalesce(sum($"mdr_charges"),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    INSAuto.union(INSNonAuto).union(CreditLifeInsurance)
      .select(
        $"report_date",
        $"customer",
        $"business_partner",
        $"product",
        $"product_category",
        $"payment_channel",
        $"no_of_transactions".cast(IntegerType),
        $"no_of_policy".cast(IntegerType),
        $"no_of_insurance_coverage".cast(IntegerType),
        $"gross_written_premium".cast(DecimalType(18,4)),
        $"commission".cast(DecimalType(18,4)),
        $"discount".cast(DecimalType(18,4)),
        $"premium".cast(DecimalType(18,4)),
        $"unique_code".cast(DecimalType(18,4)),
        $"coupon".cast(DecimalType(18,4)),
        $"mdr_charges".cast(DecimalType(18,4))
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
