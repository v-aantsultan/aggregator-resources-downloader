package com.eci.anaplan.ic.paylater.r003.aggregations.joiners

import com.eci.anaplan.ic.paylater.r003.aggregations.constructors.{SlpCsf01DF, SlpCsf03DF, SlpCsf07DF, SlpPlutusPlt01DF, SlpPlutusPlt03DF, SlpPlutusPlt07DF}
import com.eci.common.constant.Constant
import org.apache.spark.sql.functions.{coalesce, col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class IcPaylaterR003Detail @Inject()(
                                         sparkSession: SparkSession,
                                         slpCsf01DF: SlpCsf01DF,
                                         slpCsf03DF: SlpCsf03DF,
                                         slpCsf07DF: SlpCsf07DF,
                                         slpPlutusPlt01DF: SlpPlutusPlt01DF,
                                         slpPlutusPlt03DF: SlpPlutusPlt03DF,
                                         slpPlutusPlt07DF: SlpPlutusPlt07DF
                                         ){

  import sparkSession.implicits._

  private def joinSlpCsf01DF(): DataFrame = {
    slpCsf01DF.getSpecific
      .groupBy($"report_date", $"product_category", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed"), Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )
  }

  private def joinSlpCsf03DF(): DataFrame = {
    slpCsf03DF.getSpecific
      .groupBy($"report_date", $"product_category", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed") * -1, Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )

  }

  private def joinSlpCsf07DF(): DataFrame = {
    slpCsf07DF.getSpecific
      .groupBy($"report_date", $"product_category", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed") * -1, Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )
  }

  private def joinSlpPlutusPlt01DF(): DataFrame = {
    slpPlutusPlt01DF.getSpecific
      .groupBy($"report_date", $"product_category", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed"), Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )
  }

  private def joinSlpPlutusPlt03DF(): DataFrame = {
    slpPlutusPlt03DF.getSpecific
      .groupBy($"report_date", $"product_category", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed") * -1, Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )
  }

  private def joinSlpPlutusPlt07DF(): DataFrame = {
    slpPlutusPlt07DF.getSpecific
      .groupBy($"report_date", $"product_category", $"source_of_fund", $"transaction_type")
      .agg(
        coalesce(sum($"loan_disbursed") * -1, Constant.LitZero).as("loan_disbursed")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame = {
    this.joinSlpCsf01DF()
      .union(this.joinSlpCsf03DF())
      .union(this.joinSlpCsf07DF())
      .union(this.joinSlpPlutusPlt01DF())
      .union(this.joinSlpPlutusPlt03DF())
      .union(this.joinSlpPlutusPlt07DF())
      .withColumn("loan_disbursed", col("loan_disbursed").cast("decimal(30,4)"))
  }
}
