package com.eci.anaplan.ci.details.configs

import com.eci.common.TimeUtils._
import java.time.ZonedDateTime

/**
 * Validate the start date and end date to ETL
 */
trait ETLDateValidation {
  def validateDateRange(zonedDateTimeStartDate: ZonedDateTime, zonedDateTimeEndDate: ZonedDateTime): Unit = {
    require(isZonedDateInWholeHour(zonedDateTimeStartDate), "Start date time should be in whole hour")
    require(isZonedDateInWholeHour(zonedDateTimeEndDate), "End date time should be in whole hour")

    require(dateLessOrEqualNow(zonedDateTimeStartDate),
      s"Start date time cannot be in the future: Input:[zonedDateTimeStartDate, currentDateTime]:" +
        s" [$zonedDateTimeStartDate, $zonedDateTimeEndDate]")

    require(isUtc(zonedDateTimeStartDate), s"From dateTime zone expected to be in UTC. Input:[$zonedDateTimeStartDate]")

    require(dateLessOrEqualNow(zonedDateTimeEndDate),
      s"End date time cannot be in the future: Input:[zonedDateTimeEndDate, currentDateTime]: [$zonedDateTimeEndDate, $utcDateTimeNow]")

    require(isUtc(zonedDateTimeEndDate), s"To date time zone expected to be in UTC. Input:[$zonedDateTimeEndDate]")

    require(dateLessOrEqual(zonedDateTimeStartDate, zonedDateTimeEndDate),
      s"`End date time` has to be after or equal to `Start date time`. [zonedDateTimeStartDate, zonedDateTimeEndDate]:" +
        s" [$zonedDateTimeStartDate, $zonedDateTimeEndDate]")
  }
}
