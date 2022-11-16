package com.eci.common.services

import com.eci.common.LoggerSupport
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.net.URI
import javax.inject.{Inject, Singleton}

/**
 * S3 Destination Publisher service
 */
@Singleton
class S3DestinationService @Inject()(sparkSession: SparkSession,
                                     S3SourceService: S3SourceService,
                                     fileService: FileService) extends LoggerSupport{
  /**
   * Write dataframe to 1 csv file. In order to optimize the write efficiency, spark writes to parquet
   * without coalace, before reading them again and coalace as 1 csv. Also perform delete operation on
   * the temporary parquet files

   * @return file path of csv output
   */
  def write(df: DataFrame, destination: String): String = {
    val tmpParquetFolder = destination + "/parquet"
    val finalCsvFolder = destination + "/csv"

    // Step 1: Write in default partition count in parquet
    writeParquet(df, tmpParquetFolder)

    // Step 2: Read the output parquet folder, merge and output as 1 csv
    coalesceAndWriteCsv(tmpParquetFolder, finalCsvFolder)

    val fileSystem = FileSystem.get(URI.create(destination), sparkSession.sparkContext.hadoopConfiguration)

    // Step 3: Clean up the parquet files
    fileService.deleteAllFilesInDirectory(fileSystem, tmpParquetFolder)

    // Ste 4: Find the exact generated output in step 2
    fileService.findCsvFileInFolder(fileSystem, finalCsvFolder)
      .getOrElse(throw new RuntimeException("Could not find .csv file generated in Step 2"))
  }

  private def writeParquet(df: DataFrame, path: String): Unit = {
    // Write to temp folder,
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  private def coalesceAndWriteCsv(source: String, outputPath: String): Unit = {
    S3SourceService
      .readParquet(source)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("nullValue", null)
      .csv(outputPath)
  }


  def write(df: DataFrame, destination: String, isMergeSchema: Boolean): String = {
    val tmpParquetFolder = destination + "/parquet"
    val finalCsvFolder = destination + "/csv"

    // Step 1: Write in default partition count in parquet
    writeParquet(df, tmpParquetFolder)

    // Step 2: Read the output parquet folder, merge and output as 1 csv
    coalesceAndWriteCsv(tmpParquetFolder, finalCsvFolder, isMergeSchema)

    val fileSystem = FileSystem.get(URI.create(destination), sparkSession.sparkContext.hadoopConfiguration)

    // Step 3: Clean up the parquet files
    fileService.deleteAllFilesInDirectory(fileSystem, tmpParquetFolder)

    // Ste 4: Find the exact generated output in step 2
    fileService.findCsvFileInFolder(fileSystem, finalCsvFolder)
      .getOrElse(throw new RuntimeException("Could not find .csv file generated in Step 2"))
  }

  private def coalesceAndWriteCsv(source: String, outputPath: String, isMergeSchema: Boolean): Unit = {
    sparkSession
      .read
      .option("mergeSchema", isMergeSchema)
      .parquet(source)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("nullValue", null)
      .csv(outputPath)
  }
}
