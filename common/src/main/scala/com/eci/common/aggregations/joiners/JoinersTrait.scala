package com.eci.common.aggregations.joiners

import org.apache.spark.sql.DataFrame

trait JoinersTrait {

  def joinWithColumn(): DataFrame
}
