package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import javax.inject.{Inject, Singleton}

@Singleton
class SlpPlutusPlt03DF @Inject()(
                                val sparkSession: SparkSession,
                                s3SourceService: S3SourceService
                                ) {

  import sparkSession.implicits._

  private val DANAMAS = "DANAMAS"
  private val EXTERNAL = "EXTERNAL"
  private val LOAN_REPAYMENT = "LOAN_REPAYMENT"

  lazy val SlpPlutusPlt03Src = s3SourceService.getSlpPlutusPlt03Src(false)

  def getSpecific: DataFrame = {
    SlpPlutusPlt03Src
      .filter($"source_of_fund" === DANAMAS)
      .select(
        $"transaction_date".as("report_date"),
        lit(DANAMAS).as("product_category"),
        lit(EXTERNAL).as("source_of_fund"),
        lit(LOAN_REPAYMENT).as("transaction_type"),
        $"principal_amount".as("loan_disbursed")
      )
      .withColumn("report_date", date_format(to_date(col("report_date"), "yyyy-MM-dd"), "yyyy-MM"))

  }

}
