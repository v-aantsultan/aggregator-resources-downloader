package com.eci.common.services

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.SparkSession

import javax.inject.Inject

/**
 * Utilize Hadoop FileSystem API for cleaning up or finding files
 */
class FileService @Inject()(sparkSession: SparkSession) {

  /**
   * Delete all files in directory
   */
  def deleteAllFilesInDirectory(fileSystem: FileSystem, directoryPath: String): Unit = {
    val path = new Path(directoryPath)

    if (fileSystem.exists(path)) {
      val recursive = true
      fileSystem.delete(path, recursive)
    }
  }

  /**
   * Find csv file in folder by recursively looking into each directory
   * @return exact path of first csv file found
   */
  def findCsvFileInFolder(fileSystem: FileSystem, directoryPath: String): Option[String] = {
    val recursive = true
    val iterator = fileSystem.listFiles(new Path(directoryPath), recursive)

    while (iterator.hasNext) {
      val fileStatus: LocatedFileStatus = iterator.next()
      val fileName = fileStatus.getPath.getName
      if (fileName.endsWith(".csv")) {
        return Some(fileStatus.getPath.toString)
      }
    }

    // No files found in the loop above
    None
  }
}
