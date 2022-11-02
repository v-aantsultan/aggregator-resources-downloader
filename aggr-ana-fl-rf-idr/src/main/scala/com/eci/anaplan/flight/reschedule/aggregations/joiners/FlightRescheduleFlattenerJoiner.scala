package com.eci.anaplan.flight.reschedule.aggregations.joiners

import com.eci.anaplan.flight.reschedule.aggregations.constructors.MappingFulfillmentIDDf
import org.apache.spark.sql.functions.{lower, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightRescheduleFlattenerJoiner @Inject()(spark: SparkSession,
                                                flightRescheduleMultiplierJoiner: FlightRescheduleMultiplierJoiner,
                                                mappingFulfillmentIDDf: MappingFulfillmentIDDf) {
  import spark.implicits._

  def get: DataFrame = {

    flightRescheduleMultiplierJoiner.get().as("fl")
      .join(
        mappingFulfillmentIDDf.get.as("flat_fulfillment"),
        lower($"flat_fulfillment.fulfillment_id") === lower($"fl.old_fulfillment_id"),
        "left"
      )
      .withColumn("wholesaler",
        when(
          $"flat_fulfillment.wholesaler".isNull || $"flat_fulfillment.wholesaler" === "",
          "N/A"
        ).otherwise($"flat_fulfillment.wholesaler")
      )
      .select(
        $"fl.*",
        $"wholesaler"
      )
  }
}