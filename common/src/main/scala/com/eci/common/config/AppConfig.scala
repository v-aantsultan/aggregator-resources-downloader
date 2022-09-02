package com.eci.common.config

import com.typesafe.config.Config

import javax.inject.{Inject, Singleton}

@Singleton
class AppConfig @Inject()(conf: Config) {
  import AppConfig._

  val appName: String = conf.getString(AppNameKey)

  require(appName.nonEmpty, s"$AppNameKey $NonEmptyMessage")
}

object AppConfig {
  private val prefix = "app"
  private val AppNameKey = s"$prefix.name"

  private val NonEmptyMessage = "has to be non empty"
}