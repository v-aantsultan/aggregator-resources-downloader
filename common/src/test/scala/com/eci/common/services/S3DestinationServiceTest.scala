package com.eci.common.services

import com.eci.common.{SharedBaseTest, TestSparkSession}
import org.apache.hadoop.fs.FileSystem
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.net.URI

class S3DestinationServiceTest extends SharedBaseTest with TestSparkSession {
  import testSparkSession.implicits._

  private val testDf = Seq(
    (1, "20170101"),
    (1, "20171201"),
    (1, "20180101"),
    (1, "20180201")
  ).toDF("tenant_id", "date")
  private val mockDataframeReader = mock[S3SourceService]
  private val mockFileService = mock[FileService]
  private val destination = getClass.getResource(s"/$ResourcePath").getPath
  private val fileSystem = {
    when(mockDataframeReader.readParquet(any())).thenReturn(testDf)
    FileSystem.get(URI.create(destination), testSparkSession.sparkContext.hadoopConfiguration)
  }
  val dataframeWriter = new S3DestinationService(testSparkSession,
    mockDataframeReader,
    mockFileService)

  after {
    reset(mockFileService)
  }

  "write" should "be able to read and call fileService method correctly" in {
    when(mockFileService.findCsvFileInFolder(any(), any())).thenReturn(Some(ResourcePath))

    dataframeWriter.write(testDf, destination) shouldBe ResourcePath

    verify(mockFileService, times(1)).deleteAllFilesInDirectory(fileSystem, s"$destination/parquet")
    verify(mockFileService, times(1)).findCsvFileInFolder(fileSystem, s"$destination/csv")
  }

  it should "throw Runtime exception if step 4 cannot find the csv file output" in {
    when(mockFileService.findCsvFileInFolder(any(), any())).thenReturn(None)

    a[RuntimeException] shouldBe thrownBy(dataframeWriter.write(testDf, destination))

    verify(mockFileService, times(1)).findCsvFileInFolder(fileSystem, s"$destination/csv")
  }
}

object S3DataframeWriterTest {
  private val Path = "path"
  private val ResourcePath = "MockDatalake"
}

