package com.eci.anaplan.ins.details.aggregations.constructors

import com.eci.anaplan.ins.details.services.S3SourceService
import com.eci.common.services.JsonExtractor
import org.apache.spark.sql.functions.{max, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
        $"num_of_insured",
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
        when($"pdi.num_of_coverage" === 0,$"pdi.num_of_insured")
          .otherwise($"pdi.num_of_coverage").as("num_of_coverage")
      )
  }
}
