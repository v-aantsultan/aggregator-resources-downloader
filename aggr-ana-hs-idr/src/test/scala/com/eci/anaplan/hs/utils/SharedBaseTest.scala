package com.eci.anaplan.hs.utils

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
 * Shared base test
 */
trait SharedBaseTest extends FlatSpec with SharedDataFrameStubber
  with BeforeAndAfter with Matchers {
  // TODO: Add your Base DataFrame Test Here. In Case of Connectivity It was Sales Delivery
  // You Many Want to edit this space according to your logic

}
