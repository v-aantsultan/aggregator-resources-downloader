package com.eci.anaplan.flight.reschedule.aggregations.joiners

import com.eci.anaplan.flight.reschedule.aggregations.constructors.{ExchangeRateDf, FlightRescheduleDf}
import org.apache.spark.sql.functions.{coalesce, lit, to_date}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightRescheduleMultiplierJoiner @Inject()(spark: SparkSession,
                                                 flightRescheduleDf: FlightRescheduleDf,
                                                 exchangeRateDf: ExchangeRateDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val idrExchangeRate = exchangeRateDf.get
    val tmpRescheduleRates = coalesce($"reschedule_rates.conversion_rate", lit(1))

    flightRescheduleDf.get.as("fl")
      .join(idrExchangeRate.as("reschedule_rates"),
        $"fl.currency" === $"reschedule_rates.from_currency" &&
          to_date($"fl.new_booking_issued_date".cast(StringType), "dd/MM/yyyy") === $"reschedule_rates.conversion_date",
        "left"
      )
      .select(
        $"fl.*",
        ($"fl.refund_fee" * tmpRescheduleRates).as("refund_fee_idr"),
        ($"fl.reschedule_fee" * tmpRescheduleRates).as("reschedule_fee_idr")
      )
  }

  def get(): DataFrame =
    joinDataFrames
}