package com.eci.anaplan.ic.paylater.r001.aggregations.joiners

import com.eci.anaplan.ic.paylater.r001.aggregations.constructors.SlpCsf01DF
import com.eci.common.constant.Constant
import org.apache.spark.sql.catalyst.dsl.expressions
import org.apache.spark.sql.functions.{coalesce, countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class IcPaylaterR001Detail @Inject() (
                                     sparkSession: SparkSession,
                                     slpCsf01DF: SlpCsf01DF
                                     ){
  import sparkSession.implicits._

  private def joinDataFrame(): DataFrame = {

    slpCsf01DF.getJoinTable
      .groupBy($"report_date", $"source_of_fund", $"funding", $"installment_plan")
      .agg(
        coalesce(countDistinct($"no_of_transactions"), Constant.LitZero).as("no_of_transactions"),
        coalesce(sum($"gmv"), Constant.LitZero).as("gmv"),
        coalesce(sum($"admin_fee_commission"), Constant.LitZero).as("admin_fee_commission"),
        coalesce(sum($"interest_amount"), Constant.LitZero).as("interest_amount"),
        coalesce(sum($"mdr_fee"), Constant.LitZero).as("mdr_fee"),
        coalesce(sum($"service_income"), Constant.LitZero).as("service_income")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame = {
    joinDataFrame()
  }
}
