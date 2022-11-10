package com.eci.anaplan.ic.paylater.waterfall

import com.eci.anaplan.ic.paylater.waterfall.aggregations.joiners.IcPaylaterWaterFallDetail
import com.eci.common.AnaplanCoordinator
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{S3DestinationService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.apache.spark.sql.SparkSession

import javax.inject.Inject

class IcPaylaterWaterFallCoordinator @Inject() (
                                                 sparkSession: SparkSession,
                                                 icPaylaterWaterFallDetail: IcPaylaterWaterFallDetail,
                                                 slackClient: SlackClient,
                                                 statusManagerService: StatusManagerService,
                                                 s3DestinationService: S3DestinationService,
                                                 appConfig: AppConfig,
                                                 sourceConfig: SourceConfig,
                                                 destinationConfig: DestinationConfig
                                               ) extends AnaplanCoordinator{

  def callCoordinate() = {
    coordinate(sparkSession, icPaylaterWaterFallDetail.joinWithColumn(), slackClient,
      statusManagerService, s3DestinationService, appConfig, sourceConfig, destinationConfig)
  }

}
