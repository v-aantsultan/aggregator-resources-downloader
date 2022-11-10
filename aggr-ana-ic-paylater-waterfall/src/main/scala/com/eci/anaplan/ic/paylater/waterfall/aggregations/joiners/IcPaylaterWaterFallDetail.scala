package com.eci.anaplan.ic.paylater.waterfall.aggregations.joiners

import com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors.SlpCsf01DF
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class IcPaylaterWaterFallDetail @Inject()(
                                         sparkSession: SparkSession,
                                         slpCsf01DF: SlpCsf01DF
                                         ){

  import sparkSession.implicits._

  private def joinDataFrame(): DataFrame = {
    slpCsf01DF.getSpecific
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame = {
    joinDataFrame()
  }
}
