package com.eci.common.slack

import com.eci.common.LoggerSupport
import com.eci.common.slack.SlackClient._
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.Logger
import org.apache.http.client.methods.HttpPost

import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.{HttpStatus, NameValuePair}

import scala.io.Source


class SlackClient(slackConfig: SlackConfig) extends LoggerSupport {
  private val infoBotName = s"${slackConfig.appName}-INFO"
  private val warningBotName = s"${slackConfig.appName}-WARNING"
  private val errorBotName = s"${slackConfig.appName}-ERROR"

  private val SLACK_API_URI = "https://slack.com/api/chat.postMessage"
  val builder = new URIBuilder(SLACK_API_URI)
  private val httpClient = HttpClientBuilder.create().build()

  // sends info level message
  def info(text: String, exception: Throwable = None.orNull): Unit = {
    val queryPairs = if (exception == null) createQueryParams(text, InfoEmoji, infoBotName)
    else createQueryParamsWithExceptions(text, InfoEmoji, infoBotName, exception)
    builder.setParameters(queryPairs: _*)
    val postMethod = new HttpPost(builder.build())
    sendMessage(postMethod)
  }

  // sends warning level message
  def warn(text: String, exception: Throwable = None.orNull): Unit = {
    val queryPairs = if (exception == null) createQueryParams(text, WarningEmoji, warningBotName)
    else createQueryParamsWithExceptions(text, WarningEmoji, warningBotName, exception)
    builder.setParameters(queryPairs: _*)
    val postMethod = new HttpPost(builder.build())
    sendMessage(postMethod)
  }

  // sends error level message
  def error(text: String, exception: Throwable = None.orNull): Unit = {
    val queryPairs = if (exception == null) createQueryParams(text, ErrorEmoji, errorBotName)
    else createQueryParamsWithExceptions(text, ErrorEmoji, errorBotName, exception)
    builder.setParameters(queryPairs: _*)
    val postMethod = new HttpPost(builder.build())
    sendMessage(postMethod)
  }

  // write to logs and send message. Takes in the logger that is customised in the calling class.
  def logAndNotify(message: String, logger: Logger, exception: Throwable = None.orNull): Unit = {
    logger.error(message, exception)
    this.error(message, exception)
  }

  private def createQueryParams(text: String, emoji: String, botName: String): List[NameValuePair] = {
    List(
      new BasicNameValuePair("channel", slackConfig.slackChannel),
      new BasicNameValuePair("icon_emoji", emoji),
      new BasicNameValuePair("username", botName),
      new BasicNameValuePair("text", text)
    )
  }

  private def createQueryParamsWithExceptions(text: String, emoji: String, botName: String, throwable: Throwable): List[NameValuePair] = {
    val attachments = createAttachments(throwable)
    List(
      new BasicNameValuePair("channel", slackConfig.slackChannel),
      new BasicNameValuePair("icon_emoji", emoji),
      new BasicNameValuePair("username", botName),
      new BasicNameValuePair("text", text),
      new BasicNameValuePair("attachments", attachments)
    )
  }

  private def createAttachments(throwable: Throwable) = {
    val mapper = new ObjectMapper
    val attachment = mapper.createObjectNode
    attachment.put("title", "Exception:")
    attachment.put("text", throwable.toString)
    val attachments = mapper.createArrayNode
    attachments.add(attachment)
    attachments.toString
  }

  private def sendMessage(method: HttpPost): Unit = {
    try {
      method.addHeader("Authorization", s"Bearer ${slackConfig.botToken}")
      val httpResponse = httpClient.execute(method)
      val statusCode = httpResponse.getStatusLine.getStatusCode
      val responseString = Source.fromInputStream(httpResponse.getEntity.getContent).mkString
      if (statusCode != HttpStatus.SC_OK) {
        logger.error("Slack API returned non 200 response: [Code, Response]: [" + statusCode + ", " + responseString + "]")
      }
    } catch {
      case unexpectedException: Exception =>
        logger.error("Could not send message Slack message: ", unexpectedException)
    } finally method.releaseConnection()
  }
}

object SlackClient {
  val InfoEmoji = ":information_source:"
  val WarningEmoji = ":warning:"
  val ErrorEmoji = ":x:"
}