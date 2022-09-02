package com.eci.common.providers

import com.eci.common.SharedBaseTest
import com.eci.common.config.{AppConfig, Environment}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}

class SparkSessionProviderTest extends SharedBaseTest {
  import SparkSessionProviderTest._

  private val mockConfig = mock[Config]
  private val mockAppConfig = mock[AppConfig]

  "constructor" should "look for aws config in local env" in {
    when(mockAppConfig.appName).thenReturn("app-name")
    when(mockConfig.getString(any())).thenReturn("val")

    new SparkSessionProvider(Environment.LOCAL, mockAppConfig, mockConfig).get shouldBe a[SparkSession]
    verify(mockConfig, times(1)).getString(AwsAccessKeyIdKey)
    verify(mockConfig, times(1)).getString(AwsSecretAccessKey)
    verify(mockConfig, times(1)).getString(AwsSessionTokenKey)
  }
}

object SparkSessionProviderTest {
  val AwsAccessKeyIdKey = "aws-access-key-id"
  val AwsSecretAccessKey = "aws-secret-access-key"
  val AwsSessionTokenKey = "aws-session-token"
}
