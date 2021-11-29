package com.eci.anaplan.configs

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit
import com.eci.common.config.Environment
import com.eci.common.TimeUtils.utcZonedDateTime
import com.eci.common.config.Environment.Environment
import com.typesafe.config.ConfigFactory
import javax.inject.{Inject, Singleton}
import scala.concurrent.duration.Duration

/**
 * Define the config variable loaded when application starts up
 */
@Singleton
class Config @Inject()(env: Environment) extends ETLDateValidation {
  private val configFileName = "aggr-ana-gv-salesb2c-idr"

  private var conf = env match {
    case Environment.LOCAL =>
      ConfigFactory.load(s"$configFileName-$env")
    case _ =>
      ConfigFactory.load(configFileName)
  }

  /**
   * Current running environment
   */
  val environment = env

  /**
   * Start date and end date of the time window to process data.
   *
   * In case of Anaplan, it should be booking issue date or delivery timestamp.
   *
   * Other reports might deal with check in dates or whichever date window they are interested in to ETL
   */
  val (utcZonedStartDate, utcZonedEndDate): (ZonedDateTime, ZonedDateTime) = {
    val startDate = conf.getString("start-date")
    val endDate = conf.getString("end-date")

    val zonedDateTimeStartDate = utcZonedDateTime(startDate)
    val zonedDateTimeEndDate = utcZonedDateTime(endDate)

    validateDateRange(zonedDateTimeStartDate, zonedDateTimeEndDate)
    (zonedDateTimeStartDate, zonedDateTimeEndDate)
  }

  /**
   * Only used on local development mode
   */
  lazy val awsAccessKeyId: String = conf.getString("aws-access-key-id")
  lazy val awsSecretAccessKey: String = conf.getString("aws-secret-access-key")
  lazy val awsSessionToken: String = conf.getString("aws-session-token")

  /**
   * The mode which spark will starts with. local environment should use local[*]
   * and EMR cluster should use yarn.
   */
  val sparkMode: String = if (env == Environment.LOCAL) "local[*]" else "yarn"

  /**
   * The bucket name of aggregator output (CSV)
   */
  val aggregatorDest: String = conf.getString("aggregator-dest")

  /**
   * The data lake source path
   */
  val flattenerSrc: String = conf.getString("flattener-src")
  val flattenerSrcLocal: String = conf.getString("flattener-src-local")

  /**
   * The schema name in data warehouse
   */
  val schemaName: String = conf.getString("schema-name")

  /**
   * The table name in data warehouse
   */
  val tableName: String = conf.getString("table-name")

  /**
   * Give the spark job an app name
   */
  lazy val sparkAppName: String = s"Aggregator for $schemaName-$tableName"

  /**
   * The partition key to split the reports into many deltas
   * For example, Connectivity table is built from many booking issue date ranges,
   * So the key is booking_issue_date, which is the column in the warehouse table
   */
  val partitionKey: String = conf.getString("partition-key")

  /**
   * Status Manager properties
   */
  val statusManagerUrl: String = conf.getString("statusmanager.url")
  val statusManagerUsername: String = conf.getString("statusmanager.username")
  val statusManagerPassword: String = conf.getString("statusmanager.password")
  val statusManagerTimeout: Duration = Duration(conf.getDuration("statusmanager.timeout").getSeconds, TimeUnit.SECONDS)
}
