package com.eci.anaplan.ins.nonauto.providers

import com.eci.anaplan.ins.nonauto.configs.Config
import com.eci.common.config.Environment.Environment
import com.eci.common.slack.{DefaultSlackClientFactory, SlackClient, SlackConfig}
import com.google.inject.Provider
import javax.inject.Inject

class SlackClientProvider @Inject()(config: Config, environment: Environment) extends Provider[SlackClient] {

  override def get(): SlackClient = {
    val slackConfig = SlackConfig(config.slackChannelKey, config.slackBotToken, config.appName)
    DefaultSlackClientFactory.create(slackConfig)
  }
}

object SlackClientProvider {
  val SlackChannelKey = "slack.channel"
  val SlackBotToken = "slack.token"
  val AppNameKey = "app.name"
}