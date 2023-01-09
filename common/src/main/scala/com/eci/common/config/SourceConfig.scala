package com.eci.common.config

import com.eci.common.TimeUtils
import com.eci.common.TimeUtils._
import com.google.common.base.MoreObjects
import com.typesafe.config.Config

import java.time.ZonedDateTime
import javax.inject.{Inject, Singleton}

@Singleton
class SourceConfig @Inject()(conf: Config) {
  import SourceConfig._

  private val fromDate = conf.getString(FromDateKey)
  private val toDate = conf.getString(ToDateKey)

  val zonedDateTimeFromDate: ZonedDateTime = utcZonedDateTime(fromDate)
  val zonedDateTimeToDate: ZonedDateTime = utcZonedDateTime(toDate)
  val path: String = conf.getString(PathKey)
  val dataWarehousePath: String = conf.getString(DataWarehouseKey)
  val sourceName: String = conf.getString(SourceName)
  val partitionKey: String = conf.getString(PartitionKey)

  require(TimeUtils.isZonedDateInWholeHour(zonedDateTimeFromDate), s"$FromDateKey: $zonedDateTimeFromDate  should be in whole hour")
  require(TimeUtils.isZonedDateInWholeHour(zonedDateTimeToDate), s"$ToDateKey: $zonedDateTimeToDate should be in whole hour")
  require(dateLessOrEqualNow(zonedDateTimeFromDate), s"$FromDateKey val: $fromDate ($zonedDateTimeFromDate) cannot be in the future.")
  require(dateLessOrEqualNow(zonedDateTimeToDate), s"$ToDateKey val: $toDate ($zonedDateTimeToDate) cannot be in the future.")
  require(dateLessOrEqual(zonedDateTimeFromDate, zonedDateTimeToDate),
    s"`$ToDateKey` has to be after or equal to $FromDateKey. " +
      s"[$FromDateKey, $ToDateKey]: [$fromDate ($zonedDateTimeToDate), $toDate ($zonedDateTimeToDate)]")

  require(path.nonEmpty, s"$PathKey has to be non empty")
  require(dataWarehousePath.nonEmpty, s"$DataWarehouseKey has to be non empty")

  override def toString: String = MoreObjects.toStringHelper(this)
    .add("zonedDateTimeFromDate", zonedDateTimeFromDate)
    .add("zonedDateTimeToDate", zonedDateTimeToDate)
    .add("path", path)
    .add("dataWarehousePath", dataWarehousePath)
    .toString
}

object SourceConfig {
  private val FromDateKey = "start-date"
  private val ToDateKey = "end-date"
  private val PathKey = "flattener-srcdtl"
  private val DataWarehouseKey = "flattener-src"
  private val PartitionKey = "partition-key"
  private val SourceName = "source-name"
}