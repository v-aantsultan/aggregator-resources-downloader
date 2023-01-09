package com.resources.downloader.aggregations.constructors

import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, sum}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpPlutusPlt07DF @Inject()(
                                val sparkSession: SparkSession,
                                s3SourceService: S3SourceService
                                ) extends ConstructorsTrait {

  import sparkSession.implicits._

  override def getSpecific: DataFrame = {
    s3SourceService.getSlpPlutusPlt07Src(false)
      .drop("loan_id", "write_off_id", "installment_id", "days_overdue", "total_write_off_amount")
      .select("*")
//      .withColumn("principal_amount_write_off", col("principal_amount_write_off").cast("decimal(38,18)"))

  }
}
