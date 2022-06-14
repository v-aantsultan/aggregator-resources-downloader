package com.eci.anaplan.instant.debit.details.aggregations.joiners

import com.eci.anaplan.instant.debit.details.aggregations.constructors.{PaymentScopeSheetDf, UnderlyingProductSheetDf}
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, substring, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class InstantDebitDetailsJoiner @Inject()(spark: SparkSession,
                                          InstantDebitJoiner: InstantDebitJoiner,
                                          UnderlyingProductSheetDf: UnderlyingProductSheetDf,
                                          PaymentScopeSheetDf: PaymentScopeSheetDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val ID = InstantDebitJoiner.get
    val UP = UnderlyingProductSheetDf.get
    val PS = PaymentScopeSheetDf.get

    val JoinedMapping = ID.as("InsDeb")
      .join(UP.as("UnPro"),
        ID("product_type") === UP("fs_product_type"),
        "left"
      )
      .join(PS.as("PaySco"),
        ID("payment_scope") === PS("instant_debit_payment_scope"),
        "left"
      )
      .withColumn("product_type_map",
        coalesce($"UnPro.underlying_product",$"InsDeb.product_type")
      )
      .select(
        $"InsDeb.issued_time_formatted".as("report_date"),
        lit("ID").as("product"),
        coalesce($"product_type_map",lit("None")).as("product_type"),
        substring($"InsDeb.payment_currency",1,2).as("customer"),
        $"PaySco.business_partner".as("business_partner"),
        $"InsDeb.payment_scope".as("payment_channel"),
        $"InsDeb.payment_id",
        $"InsDeb.payment_amount_idr",
        $"InsDeb.point_grant_idr",
        ($"InsDeb.mdr_charges_idr" * lit(-1)).as("mdr_charges_idr")
      )

    JoinedMapping.groupBy($"report_date",$"product",$"product_type",$"customer",$"business_partner",$"payment_channel")
      .agg(
        coalesce(countDistinct($"payment_id"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"payment_amount_idr"),lit(0)).as("gmv"),
        coalesce(sum($"point_grant_idr"),lit(0)).as("point_grant"),
        coalesce(sum($"mdr_charges_idr"),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
