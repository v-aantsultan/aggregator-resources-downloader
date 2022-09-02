package com.eci.common.config

import com.eci.common.SharedBaseTest
import com.typesafe.config.Config
import org.mockito.Mockito.when

class AppConfigTest extends SharedBaseTest{
  import AppConfigTest._

  private val mockConfig = mock[Config]

  "Constructor" should "throw illegal argument if url is empty" in {
    when(mockConfig.getString(AppNameKey)).thenReturn("")

    an[IllegalArgumentException] shouldBe thrownBy(new AppConfig(mockConfig))
  }
}

object AppConfigTest {
  private val prefix = "app"
  private val AppNameKey = s"$prefix.name"
}

