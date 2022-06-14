package com.eci.anaplan.instant.debit.details.aggregations.constructors

import com.eci.anaplan.instant.debit.details.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class UnderlyingProductSheetDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.UnderlyingProductSheetSrc
      .select(
        $"fs_product_type",
        $"underlying_product"
      )
  }
}
