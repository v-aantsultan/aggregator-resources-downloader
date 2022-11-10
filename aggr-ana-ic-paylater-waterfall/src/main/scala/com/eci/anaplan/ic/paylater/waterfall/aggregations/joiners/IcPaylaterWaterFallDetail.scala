package com.eci.anaplan.ic.paylater.waterfall.aggregations.joiners

import com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors.SlpCsf01DF
import com.eci.common.constant.Constant
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, countDistinct, sum}

import javax.inject.{Inject, Singleton}

@Singleton
class IcPaylaterWaterFallDetail @Inject()(
                                         sparkSession: SparkSession,
                                         slpCsf01DF: SlpCsf01DF
                                         ){

  import sparkSession.implicits._

  private def joinDataFrame(): DataFrame = {
    slpCsf01DF.getSpecific
      .groupBy($"report_date", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed"), Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame = {
    joinDataFrame()
  }
}
