package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.common.constant.Constant
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

  private val CHANNELING_AGENT_PAYLATER = "CHANNELING_AGENT-PAYLATER"
  private val CHANNELING_AGENT_BNI_VCC = "CHANNELING_AGENT-BNI_VCC"
  private val CSF = "CSF"
  private val ON = "ON"
  private val OF = "OF"
  private val VOID_LOAN = "VOID_LOAN"
  private val INTERNAL = "INTERNAL"
  private val NA = "N/A"

  private lazy val SlpCsf07Src = s3SourceService.getSlpCsf07Src(false, false)

  def getSpecific: DataFrame = {
    SlpCsf07Src
      .filter($"is_written_off" === "" || $"is_written_off" =!= "Y" || $"is_written_off".isNull)
      .select(
        to_date($"transaction_date").as("report_date"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, lit(CSF))
          .when($"channeling_agent_id" === CHANNELING_AGENT_BNI_VCC, lit(ON))
          .otherwise(lit(OF))
          .as("product_category"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER && $"funded_by" === CSF, lit(INTERNAL))
          .when($"channeling_agent_id" === CHANNELING_AGENT_BNI_VCC && $"funded_by" =!= CSF, lit($"funded_by"))
          .otherwise(lit(NA))
          .as("source_of_fund"),
        coalesce($"write_off_type", lit(VOID_LOAN)).as("transaction_type"),
        coalesce($"write_off_principal_amount", lit(Constant.LitZero)).as("loan_disbursed")
      )
      .withColumn("report_date", date_format(to_date(col("report_date"), "yyyy-MM-dd"), "yyyy-MM"))
  }
}
