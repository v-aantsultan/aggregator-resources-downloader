package com.eci.anaplan.flight.details.aggregations.joiners

import com.eci.anaplan.flight.details.aggregations.constructors.{FlightDf, MappingAffiliateIdDf, MappingAirlineIdDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import javax.inject.{Inject, Singleton}

@Singleton
class FlightFlattenerJoiner @Inject()(spark: SparkSession,
                                      flightDataFrame: FlightDf,
                                      mappingAffiliateIdDf: MappingAffiliateIdDf,
                                      mappingAirlineIdDf: MappingAirlineIdDf) {

  import spark.implicits._

  def get: DataFrame = {
    flightDataFrame.get.as("fl")
      .join(mappingAirlineIdDf.get.as("al"),
        $"fl.airline_id" === $"al.airline_id",
        "left"
      )
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
      .withColumn("airline_id_col",
        when($"al.mapping_airline_id" === "N/A" || $"al.mapping_airline_id".isNull, $"fl.airline_id")
          .otherwise($"al.mapping_airline_id")
      )

      .select(
        $"fl.*",
        $"airline_id_col".as("airline_id"),
        $"customer".as("customer")
      )
      .drop($"fl.airline_id")
  }
}