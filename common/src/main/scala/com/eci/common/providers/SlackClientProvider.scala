package com.eci.common.providers

import com.eci.common.config.Environment.Environment
import com.eci.common.providers.SlackClientProvider._
import com.eci.common.slack.{DefaultSlackClientFactory, SlackClient, SlackConfig}
import com.google.inject.Provider
import com.typesafe.config.Config

import javax.inject.Inject

class SlackClientProvider @Inject()(config: Config, environment: Environment) extends Provider[SlackClient] {

  override def get(): SlackClient = {
    val slackConfig = SlackConfig(config.getString(slackChannelKey), config.getString(slackBotToken), config.getString(appNameKey))
    DefaultSlackClientFactory.create(slackConfig)
  }
}

object SlackClientProvider {
  val slackChannelKey = "slack.channel"
  val slackBotToken = "slack.token"
  val appNameKey = "app.name"
}
