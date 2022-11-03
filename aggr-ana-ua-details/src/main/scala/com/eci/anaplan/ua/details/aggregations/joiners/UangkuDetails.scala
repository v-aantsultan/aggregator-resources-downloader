package com.eci.anaplan.ua.details.aggregations.joiners

import com.eci.anaplan.ua.details.aggregations.constructors.UADetailsDf
import org.apache.spark.sql.functions.{coalesce, count, lit, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class UangkuDetails @Inject()(spark: SparkSession,
                              UADetailsDf: UADetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val UangkuDetailsFinal = UADetailsDf.get
      .groupBy($"report_date", $"type_of_transaction")
      .agg(
        coalesce(sum($"mutation_amount"),lit(0)).cast(DecimalType(18,4)).as("transaction_amount"),
        coalesce(count($"mutation_id"),lit(0)).cast(IntegerType).as("no_of_transactions")
      )
      .select(
        $"*"
      )
    UangkuDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
