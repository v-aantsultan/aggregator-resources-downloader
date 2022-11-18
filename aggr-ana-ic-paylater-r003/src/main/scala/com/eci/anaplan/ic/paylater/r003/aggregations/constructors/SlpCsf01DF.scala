package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, expr, lit, to_date, when}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsf01DF @Inject()(
                          val sparkSession: SparkSession,
                          s3SourceService: S3SourceService
                          ) {

  import sparkSession.implicits._

  private val CHANNELING_AGENT_PAYLATER = "CHANNELING_AGENT-PAYLATER"
  private val CHANNELING_AGENT_BNI_VCC = "CHANNELING_AGENT-BNI_VCC"
  private val CSF = "CSF"
  private val ON = "ON"
  private val OF = "OF"
  private val INTERNAL = "INTERNAL"
  private val NA = "N/A"

  private lazy val SlpCsf01Src = s3SourceService.getSlpCsf01Src(true)
  def getSpecific: DataFrame = {

    SlpCsf01Src
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
        lit("LOAN_DISBURSED").as("transaction_type"),
        $"transaction_amount".as("loan_disbursed")
      )
      .withColumn("report_date", date_format(to_date(col("report_date"), "yyyy-MM-dd"), "yyyy-MM"))
  }

}