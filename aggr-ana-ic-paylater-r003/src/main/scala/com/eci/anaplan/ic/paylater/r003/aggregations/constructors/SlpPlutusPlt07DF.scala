package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.functions.{col, date_format, lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpPlutusPlt07DF @Inject()(
                                val sparkSession: SparkSession,
                                s3SourceService: S3SourceService
                                ) {

  import sparkSession.implicits._

  private val DANAMAS = "DANAMAS"
  private val EXTERNAL = "EXTERNAL"
  private val WRITE_OFF = "WRITE_OFF"

  lazy val SlpPlutusPlt07Src = s3SourceService.getSlpPlutusPlt07Src(false)

  def getSpecific:DataFrame = {
    SlpPlutusPlt07Src
      .filter($"source_of_fund" === DANAMAS)
      .select(
        $"transaction_date".as("report_date"),
        lit(DANAMAS).as("product_category"),
        lit(EXTERNAL).as("source_of_fund"),
        lit(WRITE_OFF).as("transaction_type"),
        $"principal_amount_write_off".as("loan_disbursed")
      )
      .withColumn("report_date", date_format(to_date(col("report_date"), "yyyy-MM-dd"), "yyyy-MM"))
  }
}
