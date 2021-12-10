package com.eci.common.slack

object DefaultSlackClientFactory {
  def create(slackConfig: SlackConfig) = new SlackClient(slackConfig)
}