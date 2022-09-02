package com.eci.common.config

import com.traveloka.eci.statusmanager.api.models.config.StatusManagerConfig
import com.typesafe.config.Config

import javax.inject.{Inject, Singleton}
import scala.concurrent.duration._

@Singleton
class StatusDbConfig @Inject()(config: Config) {
  import StatusDbConfig._

  private val url: String = config.getString(UrlPath)
  private val user: String = config.getString(UserPath)
  private val password: String = config.getString(PasswordPath)

  require(url.nonEmpty, s"source url $NonEmptyMessage")
  require(user.nonEmpty, s"source user $NonEmptyMessage")

  val statusManagerConfig = StatusManagerConfig(user, password, url)
  val requestDuration: FiniteDuration = 10 seconds
}

object StatusDbConfig {
  private val prefix = "statusmanager"

  val UrlPath = s"$prefix.url"
  val UserPath = s"$prefix.username"
  val PasswordPath = s"$prefix.password"

  val NonEmptyMessage = "cannot be empty"
}