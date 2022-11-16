package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.common.constant.Constant
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.functions.{expr, lit, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsf01DF @Inject()(
                                   val sparkSession: SparkSession,
                                   s3SourceService: S3SourceService
                                 ){
  import sparkSession.implicits._

  lazy val SlpCsf01Src = s3SourceService.getSlpCsf01Src(true)

  def getSpecific: DataFrame = {

    val CHANNELING_AGENT_PAYLATER = "CHANNELING_AGENT-PAYLATER"
    val CHANNELING_AGENT_BNI_VCC = "CHANNELING_AGENT-BNI_VCC"

    SlpCsf01Src
      .select(
        to_date($"transaction_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, lit("CSF"))
          .when($"channeling_agent_id" === CHANNELING_AGENT_BNI_VCC, lit("ON"))
          .otherwise("OF").as("product_category"),
        when(($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER) && ($"funded_by" === "CSF"), lit("INTERNAL"))
        .when(($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER) && ($"funded_by" !=  "CSF"), lit ($"funded_by"))
        .otherwise("N/A").as("source_of_fund"),
        $"installment_plan".as("installment_plan"),
        $"loan_id".as("no_of_transactions"),
        $"transaction_amount".as("gmv"),
        when($"channeling_agent_id" === CHANNELING_AGENT_PAYLATER, Constant.LitZero)
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
        $"product_category".as("product_category"),
        $"source_of_fund".as("source_of_fund"),
        $"installment_plan".as("installment_plan"),
        $"no_of_transactions".as("no_of_transactions"),
        $"gmv".as("gmv"),
        $"admin_fee_commission".as("admin_fee_commission"),
        $"interest_amount".as("interest_amount"),
        $"mdr_fee".as("mdr_fee"),
        $"service_income".as("service_income"),
        ($"user_acquisition_fee" * lit(-1)).as("user_acquisition_fee"),
        when($"product_type" === $"fs_product_type", lit($"underlying_product"))
          .otherwise($"product_type")
          .as("product")
    )
  }
}
