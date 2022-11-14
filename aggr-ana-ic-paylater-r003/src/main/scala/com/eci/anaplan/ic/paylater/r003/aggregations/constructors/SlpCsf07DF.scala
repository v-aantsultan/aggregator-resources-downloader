package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsf07DF @Inject ()(
                           val sparkSession: SparkSession,
                           s3SourceService: S3SourceService
                           ) {

  import sparkSession.implicits._

  private val CHANNELING = "CHANNELING"
  private val CHANNELING_BNI = "CHANNELING-BNI"
  private val SELF_FUNDING = "SELF-FUNDING"
  private val CHANNELING_AGENT_PAYLATER = "CHANNELING_AGENT-PAYLATER"
  private val CHANNELING_AGENT_BNI_VCC = "CHANNELING_AGENT-BNI_VCC"
  private val CSF = "CSF"
  private val ON = "ON"
  private val OF = "OF"
  private val VOID_LOAN = "VOID_LOAN"

  def getSpecific: DataFrame = {
    s3SourceService.SlpCsf07Src
      .filter($"is_written_off" === "" || $"is_written_off" =!= "Y" || $"is_written_off".isNull)
      .select(
        to_date($"transaction_date").as("report_date"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, lit(CSF))
          .when($"channeling_agent_id" === CHANNELING_AGENT_BNI_VCC, lit(ON))
          .otherwise(lit(OF))
          .as("product_category"),
        when($"source_of_fund" === CHANNELING, lit(CHANNELING_BNI))
          .otherwise(lit(SELF_FUNDING))
          .as("source_of_fund"),
        coalesce($"write_off_type", lit(VOID_LOAN)).as("transaction_type"),
        coalesce($"write_off_principal_amount", lit(0).cast(DecimalType(30,4))).as("loan_disbursed")
      )
      .withColumn("report_date", date_format(to_date(col("report_date"), "yyyy-MM-dd"), "yyyy-MM"))
  }
}
