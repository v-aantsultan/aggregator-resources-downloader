package com.eci.anaplan.instant.debit.details.aggregations.constructors

import com.eci.anaplan.instant.debit.aggregations.joiners.InstantDebitJoiner
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class InstantDebitDf @Inject()(val sparkSession: SparkSession, InstantDebitJoiner: InstantDebitJoiner) {

  import sparkSession.implicits._

  def get: DataFrame = {

    InstantDebitJoiner.joinWithColumn()
      .select(
        $"issued_time_formatted",
        substring($"product_type",1,2).as("product_type"),
        $"payment_currency",
        $"payment_scope",
        $"payment_id",
        $"payment_amount_idr",
        $"point_grant_idr",
        $"mdr_charges_idr"
      )
  }
}