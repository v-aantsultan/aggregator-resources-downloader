package com.eci.common

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

/**
 * Shared base test
 */
trait SharedBaseTest extends FlatSpec with MockitoSugar with BeforeAndAfterAll with BeforeAndAfterEach with TestSparkSession
  with BeforeAndAfter with Matchers {

  def orderBy(df: DataFrame, col: String): DataFrame = {
    df.orderBy(col)
  }

  def orderBy(df: DataFrame, col1: String, col2: String): DataFrame = {
    df.orderBy(col1, col2)
  }
}

