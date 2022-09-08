package com.eci.anaplan.crn.nrd.aggr.providers

import com.eci.anaplan.crn.nrd.aggr.configs.Config
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