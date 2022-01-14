package com.eci.anaplan.providers

import com.eci.anaplan.configs.LPSummaryConfig
import com.eci.common.config.Environment.Environment
import com.eci.common.slack.{DefaultSlackClientFactory, SlackClient, SlackConfig}
import com.google.inject.Provider
import javax.inject.Inject

class LPSummarySlackClient @Inject()(config: LPSummaryConfig, environment: Environment) extends Provider[SlackClient] {

  override def get(): SlackClient = {
    val slackConfig = SlackConfig(config.slackChannelKey, config.slackBotToken, config.appName)
    DefaultSlackClientFactory.create(slackConfig)
  }
}