package com.eci.anaplan.ins.sum.aggregations.joiners

import com.eci.anaplan.ins.sum.aggregations.constructors.{CreditLifeInsuranceDf, INSAutoDf, INSNonAutoDf}
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class InsuranceSummary @Inject()(spark: SparkSession,
                                 INSAutoDf: INSAutoDf,
                                 INSNonAutoDf: INSNonAutoDf,
                                 CreditLifeInsuranceDf: CreditLifeInsuranceDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val INSAuto = INSAutoDf.get
      .groupBy($"report_date", $"customer", $"product", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions")
      )
      .select(
        $"*"
      )

    val INSNonAuto = INSNonAutoDf.get
      .groupBy($"report_date", $"customer", $"product", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions")
      )
      .select(
        $"*"
      )

    val CreditLifeInsurance = CreditLifeInsuranceDf.get
      .groupBy($"report_date", $"customer", $"product", $"payment_channel")
      .agg(
        coalesce(countDistinct($"loan_id"),lit(0)).as("no_of_transactions")
      )
      .select(
        $"*"
      )

    INSAuto.union(INSNonAuto).union(CreditLifeInsurance)
      .groupBy($"report_date", $"customer", $"product", $"payment_channel")
      .agg(
        coalesce(sum($"no_of_transactions"),lit(0)).as("no_of_transactions")
      )
      .select(
        $"*"
      )

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
