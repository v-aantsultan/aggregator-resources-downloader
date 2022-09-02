package com.eci.common.exception

final case class StatusManagerException(message: String, cause: Throwable) extends Exception(message, cause)
