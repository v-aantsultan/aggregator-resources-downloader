package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{exp, expr, lit, size, split, to_date, when}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsf01DF @Inject()(
                                   val sparkSession: SparkSession,
                                   s3SourceService: S3SourceService
                                 ){
  import sparkSession.implicits._

  def getSpecific: DataFrame = {

    val CHANNELING_AGENT_PAYLATER = "CHANNELING_AGENT-PAYLATER"

    s3SourceService.SlpCsf01Src
      .select(
        to_date($"transaction_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, lit("CSF"))
          .when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, lit("ON"))
          .otherwise("OF").as("source_of_fund"),
        when(($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER) && ($"funded_by" === "CSF"), lit("INTERNAL"))
        .when(($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER) && ($"funded_by" !=  "CSF"), lit ($"funded_by"))
        .otherwise("N/A").as("funding"),
        $"installment_plan".as("installment_plan"),
        $"loan_id".as("no_of_transactions"),
        $"transaction_amount".as("gmv"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, lit(0))
          .otherwise($"commission_amount")
          .as("admin_fee_commission"),
        $"interest_amount_after_subsidy".as("interest_amount"),
        $"mdr_amount".as("mdr_fee"),
        $"user_acquisition_fee".as("service_income"),
        $"user_acquisition_fee".as("user_acquisition_fee"),
        $"product_type".as("product_type")
      )
  }

  def getJoinTable: DataFrame = {
    val mappingUnderlyingProductDF : MappingUnderlyingProductDF = new MappingUnderlyingProductDF(sparkSession, s3SourceService)
    val joinDF1 = this.getSpecific
      .as("slp")
      .join(mappingUnderlyingProductDF.getData
        .as("map"),
        $"slp.product_type" === $"map.fs_product_type",
        "left")

    // final join
    joinDF1
      .select(
        $"report_date".as("report_date"),
        $"source_of_fund".as("source_of_fund"),
        $"funding".as("funding"),
        $"installment_plan".as("installment_plan"),
        $"no_of_transactions".as("no_of_transactions"),
        $"gmv".as("gmv"),
        $"admin_fee_commission".as("admin_fee_commission"),
        $"interest_amount".as("interest_amount"),
        $"mdr_fee".as("mdr_fee"),
        $"service_income".as("service_income"),
        $"user_acquisition_fee".as("user_acquisition_fee"),
        when($"product_type" === $"fs_product_type", lit($"underlying_product"))
          .otherwise($"product_type")
          .as("product")
    )
  }
}
