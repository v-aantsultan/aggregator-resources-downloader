package com.eci.common.services

import com.eci.common.{SharedBaseTest, TestSparkSession}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

class FileServiceTest extends SharedBaseTest with TestSparkSession {
  import FileServiceTest._

  private val fileService = new FileService(testSparkSession)
  private val mockFileSystem = mock[FileSystem]
  private val path = new Path(DirectoryPath)

  after {
    reset(mockFileSystem)
  }

  "deleteFileInDirectory" should "call file system delete method if directory exist" in {
    when(mockFileSystem.exists(any())).thenReturn(true)

    fileService.deleteAllFilesInDirectory(mockFileSystem, DirectoryPath)

    verify(mockFileSystem, times(1)).exists(path)
    verify(mockFileSystem, times(1)).delete(path, true)
  }

  "deleteFileInDirectory" should "not call file system delete method if directory exist" in {
    when(mockFileSystem.exists(any())).thenReturn(false)

    fileService.deleteAllFilesInDirectory(mockFileSystem, DirectoryPath)

    verify(mockFileSystem, times(0)).delete(path, true)
  }

  "findCsvFileInFolder" should "throw RuntimeException when no csv file in folder" in {
    val noFilesIterator: RemoteIterator[LocatedFileStatus] = getFileIterators(Seq())

    when(mockFileSystem.listFiles(any(), any())).thenReturn(noFilesIterator)

    fileService.findCsvFileInFolder(mockFileSystem, DirectoryPath) shouldBe None

    verify(mockFileSystem, times(1)).listFiles(path, true)
  }

  "findCsvFileInFolder" should "return the csv file path if it exist" in {
    val mockFile = mock[LocatedFileStatus]
    when(mockFile.getPath).thenReturn(new Path(CsvPath))

    val oneFileIterator: RemoteIterator[LocatedFileStatus] = getFileIterators(Seq(mockFile))
    when(mockFileSystem.listFiles(any(), any())).thenReturn(oneFileIterator)

    fileService.findCsvFileInFolder(mockFileSystem, DirectoryPath) shouldBe Some(CsvPath)
    verify(mockFileSystem, times(1)).listFiles(path, true)
  }

  /**
   * Helper method to get a file iterator
   * @param mockFileList - list of files to return in the iterator
   */
  private def getFileIterators(mockFileList: Seq[LocatedFileStatus]) = new RemoteIterator[LocatedFileStatus] {
    var counter = 0

    def hasNext: Boolean = counter != mockFileList.length

    def next(): LocatedFileStatus = {
      counter += 1
      mockFileList(counter - 1)
    }

  }
}

object FileServiceTest {
  private val DirectoryPath = "DirectoryPath"
  private val CsvPath = s"$DirectoryPath/x.csv"
}
