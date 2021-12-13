package com.eci.anaplan.providers

import com.eci.anaplan.configs.GVB2CConfig
import com.eci.common.config.Environment.Environment
import com.eci.common.slack.{DefaultSlackClientFactory, SlackClient, SlackConfig}
import com.google.inject.Provider

import javax.inject.Inject

class GVB2CSlackClient @Inject()(config: GVB2CConfig, environment: Environment) extends Provider[SlackClient] {

  override def get(): SlackClient = {
    val slackConfig = SlackConfig(config.slackChannelKey, config.slackBotToken, config.appName)
    DefaultSlackClientFactory.create(slackConfig)
  }
}