package com.eci.anaplan.ic.paylater.r001.aggregations.joiners

import com.eci.anaplan.ic.paylater.r001.aggregations.constructors.IcPaylaterR001DF
import com.eci.common.constant.Constant
import org.apache.spark.sql.catalyst.dsl.expressions
import org.apache.spark.sql.functions.{coalesce, countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class IcPaylaterR001Detail @Inject() (
                                     sparkSession: SparkSession,
                                     icPaylaterR001DF: IcPaylaterR001DF
                                     ){
  import sparkSession.implicits._

  private def joinDataFrame(): DataFrame = {
    icPaylaterR001DF.getAllPeriod
      // .groupBy($"report_date")
      .agg(
        coalesce(countDistinct($"no_of_transactions"), Constant.LitZero).as("$no_of_transactions")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame = {
    joinDataFrame()
  }
}
