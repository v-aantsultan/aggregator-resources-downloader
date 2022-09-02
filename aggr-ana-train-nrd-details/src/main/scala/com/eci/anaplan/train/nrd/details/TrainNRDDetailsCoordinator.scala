package com.eci.anaplan.train.nrd.details

import com.eci.anaplan.train.nrd.details.aggregations.joiners.TrainNRDDetails
import com.eci.common.services.StatusManagerService
import com.eci.common.AnaplanCoordinator
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.S3DestinationService
import com.eci.common.slack.SlackClient
import org.apache.spark.sql.SparkSession

import javax.inject.{Inject, Named}

/**
 * The main class to perform all the actions to query data, filter, join and upload to s3 as a single CSV file.
 * It should be configured properly from the main class.
 */
class TrainNRDDetailsCoordinator @Inject()(spark: SparkSession,
                                           destinationConfig: DestinationConfig,
                                           appConfig: AppConfig,
                                           sourceConfig: SourceConfig,
                                           @Named("TENANT_ID") tenantId: String,
                                           anaplanDataframeJoiner: TrainNRDDetails,
                                           statusManagerService: StatusManagerService,
                                           s3DestinationService: S3DestinationService,
                                           slack: SlackClient) extends AnaplanCoordinator {
  def callCoordinate(): Unit = {
    coordinate(spark, anaplanDataframeJoiner.joinWithColumn(), slack,
      statusManagerService, s3DestinationService, appConfig, sourceConfig, destinationConfig
    )
  }
}
