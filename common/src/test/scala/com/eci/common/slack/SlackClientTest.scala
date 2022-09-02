package com.eci.common.slack

import com.eci.common.slack.SlackClientTest._
import com.eci.common.{LoggerSupport, SharedBaseTest}
import org.mockito.Mockito._

class SlackClientTest extends SharedBaseTest with LoggerSupport {
  private val mockSlackConfig = mock[SlackConfig]
  private val slackClient: SlackClient = new SlackClient(mockSlackConfig)

  override def beforeAll(): Unit = {
    when(mockSlackConfig.appName).thenReturn(testAppName)
    when(mockSlackConfig.slackChannel).thenReturn(testChannelName)

  }

  "SlackClient" should "be able to send an info level message" in {
    noException shouldBe thrownBy(slackClient.info(testMessage))
  }

  it should "be able to send an info level message with exception" in {
    noException shouldBe thrownBy(slackClient.info(testMessage, testException))
  }

  it should "be able to send a warning level with exception" in {
    noException shouldBe thrownBy(slackClient.warn(testMessage, testException))
  }

  it should "be able to send an error level message with exception" in {
    noException shouldBe thrownBy(slackClient.error(testMessage, testException))
  }

  "SlackClient.logAndNotify" should "be able to take a string message, a logger and an exception" in {
    noException shouldBe thrownBy(slackClient.logAndNotify(testMessage, logger, testException))
  }

}

object SlackClientTest {
  private val testChannelName = "test-channel-name"
  private val testAppName = "testApp"
  private val testMessage = "testMessage"
  private val testException = new Throwable("test exception")

}
