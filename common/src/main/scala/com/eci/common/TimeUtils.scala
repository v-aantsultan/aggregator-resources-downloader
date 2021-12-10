package com.eci.common

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

object TimeUtils {
  private val utcZoneId: ZoneId = ZoneId.of("UTC")

  val utcDateTimeNow: ZonedDateTime = ZonedDateTime.now(utcZoneId)

  def convertToUtcZoneDateTime(timestamp: Timestamp): ZonedDateTime = {
    timestamp.toInstant.atZone(utcZoneId)
  }

  def isZonedDateInWholeHour(zonedDateTime: ZonedDateTime): Boolean = {
    val ONE_HOUR_IN_SEC = 3600
    zonedDateTime.toInstant.getEpochSecond % ONE_HOUR_IN_SEC == 0
  }

  /**
   *  Convert a zoned date time to timestamp
   */
  def toTimestamp(zoneDateTime: ZonedDateTime): Timestamp = {
    if (zoneDateTime == null) null
    else Timestamp.from(zoneDateTime.toInstant)
  }

  /**
   *  Convert a date to use UTC zone
   *
   * @param zonedDateTime ZonedDateTime format
   * @return ZonedDateTime with UTC zone
   */
  def convertToUtcZone(zonedDateTime: ZonedDateTime): ZonedDateTime = {
    zonedDateTime.withZoneSameInstant(utcZoneId)
  }

  /**
   *  Override the zone with UTC and keep the date time as original
   *
   * @param dateTime ISO date format
   * @return UTC zoned date time
   */
  def utcZonedDateTime(dateTime: String): ZonedDateTime =
    ZonedDateTime.parse(dateTime, DateTimeFormatter.ISO_ZONED_DATE_TIME.withZone(utcZoneId))

  /**
   *  Format a zoned date time to yyyyMMdd
   *
   * @param dateTime ZonedDateTime format
   * @return String
   */
  def utcDateTimeString(dateTime: ZonedDateTime): String = {
    val format = DateTimeFormatter.ofPattern("yyyyMMdd")
    format.format(dateTime)
  }

  def utcDateTimeStringReport(dateTime: ZonedDateTime): String = {
    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    format.format(dateTime)
  }

  /**
   *  Determine if a date is before or equal now
   *
   * @param zonedDateTime ZonedDateTime format
   * @return Boolean
   */
  def dateLessOrEqualNow(zonedDateTime: ZonedDateTime): Boolean =
    dateLessOrEqual(zonedDateTime, utcDateTimeNow)

  /**
   *  Determine if a date is before or equal the other
   *
   * @param fromDateTime ZonedDateTime format
   * @param toDateTime ZonedDateTime format
   * @return Boolean
   */
  def dateLessOrEqual(fromDateTime: ZonedDateTime, toDateTime: ZonedDateTime): Boolean =
    fromDateTime.isBefore(toDateTime) || fromDateTime.isEqual(toDateTime)

  /**
   *  Determine if a date is using UTC zone
   *
   * @param dateTime ZonedDateTime format
   * @return Boolean
   */
  def isUtc(dateTime: ZonedDateTime): Boolean =
    dateTime.getZone.equals(utcZoneId)
}
