package com.eci.anaplan.ins.nonauto.providers

import com.eci.anaplan.ins.nonauto.configs.INSNonAutoConfig
import com.eci.common.config.Environment.Environment
import com.eci.common.slack.{DefaultSlackClientFactory, SlackClient, SlackConfig}
import com.google.inject.Provider
import javax.inject.Inject

class INSNonAutoSlackClient @Inject()(config: INSNonAutoConfig, environment: Environment) extends Provider[SlackClient] {

  override def get(): SlackClient = {
    val slackConfig = SlackConfig(config.slackChannelKey, config.slackBotToken, config.appName)
    DefaultSlackClientFactory.create(slackConfig)
  }
}

object INSNonAutoSlackClient {
  val SlackChannelKey = "slack.channel"
  val SlackBotToken = "slack.token"
  val AppNameKey = "app.name"
}