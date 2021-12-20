package com.eci.anaplan.ins.nonauto.aggregations.constructors

import com.eci.anaplan.ins.nonauto.services.S3SourceService
import com.eci.common.services.JsonExtractor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.max
import javax.inject.{Inject, Singleton}

@Singleton
class PurchaseDeliveryItemDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {

    val pdi = s3SourceService.PurchaseDeliveryItemDf
      .withColumn("policy_id",
        JsonExtractor.extractString("insuranceBookingItem.policyId")($"`additional_data`")
      )
      .withColumn("num_of_adults",
        JsonExtractor.extractString("insuranceAdditionalData.numOfAdults")($"`additional_data`")
      )
      .withColumn("num_of_infants",
        JsonExtractor.extractString("insuranceAdditionalData.numOfInfants")($"`additional_data`")
      )
      .withColumn("num_of_insured",
        JsonExtractor.extractString("insuranceAdditionalData.numOfInsured")($"`additional_data`")
      )
      .withColumn("num_of_children",
        JsonExtractor.extractString("insuranceAdditionalData.numOfChildren")($"`additional_data`")
      )
      .select(
        $"`purchase_delivery_id`".as("purchase_delivery_id"),
        $"`purchase_delivery_version`".as("version"),
        $"policy_id",
        $"num_of_adults",
        $"num_of_infants",
        $"num_of_insured",
        $"num_of_children",
        ($"num_of_adults" + $"num_of_children" + $"num_of_infants").as("num_of_coverage")
      )

    val latest_pdi =
      pdi.groupBy($"purchase_delivery_id")
      .agg(
        max($"version").as("version")
      )
      .select(
        $"*"
      )

    pdi.as("pdi")
      .join(latest_pdi.as("latest_pdi"),
      $"pdi.purchase_delivery_id" === $"latest_pdi.purchase_delivery_id" &&
        $"pdi.version" === $"latest_pdi.version"
    )
      .select(
        $"pdi.policy_id",
        $"pdi.num_of_adults",
        $"pdi.num_of_infants",
        $"pdi.num_of_insured",
        $"pdi.num_of_children",
        $"pdi.num_of_coverage"
      )
  }
}
