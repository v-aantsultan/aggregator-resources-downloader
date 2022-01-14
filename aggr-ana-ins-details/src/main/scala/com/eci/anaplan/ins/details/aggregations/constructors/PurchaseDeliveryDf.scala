package com.eci.anaplan.ins.details.aggregations.constructors

import com.eci.anaplan.ins.details.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class PurchaseDeliveryDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.PurchaseDeliveryDf
      .filter(
        $"`procurement.purchase_delivery.additional_data.tripType`" === "INSURANCE" &&
       $"`procurement.purchase_delivery_item.additional_data.insuranceProduct.productType`" === "AUTO"
      )
      .select(
        $"`procurement.purchase_delivery_item.additional_data.insuranceBookingItem.policyId`".as("policy_id"),
        $"`procurement.purchase_delivery_item.additional_data.insuranceadditionaldata.numofinsured`".as("num_of_coverage")
      )
  }
}
