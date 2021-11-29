package com.eci.common

import org.scalatest.{FlatSpec, Matchers}

class TimeUtilsTest extends FlatSpec with Matchers {
  behavior of "isZonedDateInWholeHour"

  it must "return false for time with minutes or lower" in {
    val dateString = "2019-02-01T01:02:03Z"
    val zonedDateTime = TimeUtils.utcZonedDateTime(dateString)

    TimeUtils.isZonedDateInWholeHour(zonedDateTime) shouldBe false
  }

  it must "return true for time with minutes or lower" in {
    val dateString = "2019-02-01T01:00:00Z"
    val zonedDateTime = TimeUtils.utcZonedDateTime(dateString)

    TimeUtils.isZonedDateInWholeHour(zonedDateTime) shouldBe true
  }
}
