package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPSummarySource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LPSummaryTransactionCategoryDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPSummarySource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.TransactionCategoryDf
      .select(
        $"`transaction_type`".as("transaction_type"),
        $"`transaction_category`".as("transaction_category")
      )
  }
}