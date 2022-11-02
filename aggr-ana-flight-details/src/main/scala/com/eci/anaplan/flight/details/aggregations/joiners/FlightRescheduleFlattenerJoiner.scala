package com.eci.anaplan.flight.details.aggregations.joiners

import com.eci.anaplan.flight.details.aggregations.constructors.{FlightRescheduleDf, MappingAirlineIdDf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightRescheduleFlattenerJoiner @Inject()(spark: SparkSession,
                                                FlightRescheduleDf: FlightRescheduleDf,
                                                mappingAirlineIdDf: MappingAirlineIdDf) {

  import spark.implicits._

  def get: DataFrame = {
    FlightRescheduleDf.get.as("flrs")
      .join(mappingAirlineIdDf.get.as("ai"),
        $"flrs.old_airline_id" === $"ai.airline_id",
        "left"
      )
      .withColumn("airline_id",
        when($"ai.mapping_airline_id".isNull || $"ai.mapping_airline_id" === "N/A", $"flrs.old_airline_id")
          .otherwise($"mapping_airline_id")
      )
      .drop($"ai.airline_id")
  }
}