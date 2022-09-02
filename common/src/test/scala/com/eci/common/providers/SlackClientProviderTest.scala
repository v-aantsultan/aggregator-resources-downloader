package com.eci.common.providers

import com.eci.common.SharedBaseTest
import com.eci.common.config.Environment
import com.eci.common.providers.SlackClientProviderTest._
import com.eci.common.slack.SlackClient
import com.typesafe.config.Config
import org.mockito.Mockito.when

class SlackClientProviderTest extends SharedBaseTest {
  private val conf = mock[Config]

  "constructor" should "create a a SlackClient" in {
    when(conf.getString(SlackChannelKey)).thenReturn(SlackChannelVal)
    when(conf.getString(SlackBotKey)).thenReturn(SlackBotVal)
    when(conf.getString(AppNameKey)).thenReturn(AppNameVal)
    val slackClientProvider = new SlackClientProvider(conf, LocalEnvironment)

    slackClientProvider.get() shouldBe a[SlackClient]
  }
}

object SlackClientProviderTest {
  private val SlackChannelKey = "slack.channel"
  private val SlackChannelVal = "slack-channel"
  private val SlackBotKey = "slack.token"
  private val SlackBotVal = "slack-token"
  private val AppNameKey = "app.name"
  private val AppNameVal = "anaplan"
  private val LocalEnvironment = Environment.LOCAL
}
