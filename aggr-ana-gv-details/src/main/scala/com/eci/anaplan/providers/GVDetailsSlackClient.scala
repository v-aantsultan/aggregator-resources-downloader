package com.eci.anaplan.providers

import com.eci.common.config.Environment.Environment
import com.eci.common.slack.{DefaultSlackClientFactory, SlackClient, SlackConfig}
import com.eci.anaplan.configs.GVDetailsConfig
import com.google.inject.Provider

import javax.inject.Inject

class GVDetailsSlackClient @Inject()(config: GVDetailsConfig, environment: Environment) extends Provider[SlackClient] {

  override def get(): SlackClient = {
    val slackConfig = SlackConfig(config.slackChannelKey, config.slackBotToken, config.appName)
    DefaultSlackClientFactory.create(slackConfig)
  }
}

object GVDetailsSlackClient {
  val SlackChannelKey = "slack.channel"
  val SlackBotToken = "slack.token"
  val AppNameKey = "app.name"
}