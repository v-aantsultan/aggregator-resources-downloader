package com.eci.anaplan.ic.paylater.r003

import com.eci.anaplan.ic.paylater.r003.aggregations.joiners.IcPaylaterR003Detail
import com.eci.common.AnaplanCoordinator
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{S3DestinationService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.apache.spark.sql.SparkSession

import javax.inject.Inject

class IcPaylaterR003Coordinator @Inject()(
                                                 sparkSession: SparkSession,
                                                 icPaylaterR003Detail: IcPaylaterR003Detail,
                                                 slackClient: SlackClient,
                                                 statusManagerService: StatusManagerService,
                                                 s3DestinationService: S3DestinationService,
                                                 appConfig: AppConfig,
                                                 sourceConfig: SourceConfig,
                                                 destinationConfig: DestinationConfig
                                               ) extends AnaplanCoordinator{

  def callCoordinate() = {
    // handling session for error Expected: int, Found: BINARY for slp-07
    sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    coordinate(sparkSession, icPaylaterR003Detail.joinWithColumn(), slackClient,
      statusManagerService, s3DestinationService, appConfig, sourceConfig, destinationConfig, false)
  }

}
