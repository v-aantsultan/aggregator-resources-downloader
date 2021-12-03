package com.eci.anaplan.services

import javax.inject.{Inject, Singleton}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.net.URI

/**
 * S3 Destination Publisher service
 */
@Singleton
class GVIssuedDestination @Inject()(sparkSession: SparkSession) {
  /**
   * Write the dataframe to the provided destination as one CSV file, then return its full path.
   */
  def publishToS3(anaplanDF: DataFrame, destination: String): String = {
    val tmpOutputFolder = destination + "/parquet"
    val finalOutputFolder = destination + "/csv"

    // Trigger action to write to s3 temp folder
    anaplanDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tmpOutputFolder)

    // Pulling back to merge into one file.
    // NOTE: We should avoid coalesce to 1 partition while transforming because it will go out of memory
    // Another way to go around this is to merge with copyMerge API but it's deprecated in Hadoop 3.x
    // To play safe we use this approach instead.
    sparkSession
      .read
      .parquet(tmpOutputFolder)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("nullValue", null)
      .csv(finalOutputFolder)

    val fileSystem = FileSystem.get(URI.create(destination), sparkSession.sparkContext.hadoopConfiguration)

    // Delete the parquet folder
    val tmpOutputFolderPath = new Path(tmpOutputFolder)
    if (fileSystem.exists(tmpOutputFolderPath)) {
      val recursive = true
      fileSystem.delete(tmpOutputFolderPath, recursive)
    }

    // Return the full path of the first csv file found
    val recursive = true
    val iterator = fileSystem.listFiles(new Path(finalOutputFolder), recursive)
    while (iterator.hasNext()) {
      val fileStatus: LocatedFileStatus = iterator.next()
      val fileName = fileStatus.getPath.getName
      if (fileName.endsWith(".csv")) {
        return fileStatus.getPath.toString
      }
    }

    throw new RuntimeException("Unable to locate the csv file")
  }
}
