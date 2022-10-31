package com.eci.anaplan.ic.paylater.r001

import com.eci.common.AnaplanCoordinator
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{S3DestinationService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.Inject

class IcPaylaterR001Coordinator @Inject() (
  sparkSession: SparkSession,
  dataFrame: DataFrame,
  slackClient: SlackClient,
  statusManagerService: StatusManagerService,
  s3DestinationService: S3DestinationService,
  appConfig: AppConfig,
  sourceConfig: SourceConfig,
  destinationConfig: DestinationConfig
) extends AnaplanCoordinator{

  def callCoordinate() = {
    coordinate(sparkSession, dataFrame, slackClient,
      statusManagerService, s3DestinationService, appConfig, sourceConfig, destinationConfig)
  }
}
