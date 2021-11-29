package com.eci.common.config

object Environment extends Enumeration {
  type Environment = Value
  val LOCAL = Value("local")
  val DEVELOPMENT = Value("development")
  val STAGING = Value("staging")
  val PRODUCTION = Value("production")
}
