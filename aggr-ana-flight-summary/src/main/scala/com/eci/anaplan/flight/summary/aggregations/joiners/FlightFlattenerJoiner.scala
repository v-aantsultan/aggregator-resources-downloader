package com.eci.anaplan.flight.summary.aggregations.joiners

import com.eci.anaplan.flight.summary.aggregations.constructors.{FlightDf, MappingAffiliateIdDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import javax.inject.{Inject, Singleton}

@Singleton
class FlightFlattenerJoiner @Inject()(
                                       spark: SparkSession,
                                       flightDataFrame: FlightDf,
                                       mappingAffiliateIdDf: MappingAffiliateIdDf
                                     ) {

  import spark.implicits._

  def get: DataFrame = {
    flightDataFrame.get.as("fl")
      .join(mappingAffiliateIdDf.get.as("af"),
        $"fl.affiliate_id" === $"af.affiliate_id",
        "left"
      )

      .withColumn("mapping_affiliate",
        when($"af.mapping_affiliate_id" === "N/A" || $"af.mapping_affiliate_id".isNull, lit("Affiliates Others"))
          .otherwise($"af.mapping_affiliate_id")
      )
      .withColumn("customer",
        when(!$"fl.corporate_type".isin("N/A","ERROR"), $"fl.corporate_type")
          .when(!$"fl.affiliate_id".isin("N/A","ERROR"), $"mapping_affiliate")
          .otherwise(substring($"fl.locale", -2, 2))
      )

      .select(
        $"fl.*",
        $"customer".as("customer")
      )
  }
}