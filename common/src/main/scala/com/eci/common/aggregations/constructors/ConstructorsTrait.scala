package com.eci.common.aggregations.constructors

import org.apache.spark.sql.DataFrame

trait ConstructorsTrait {

  def getSpecific: DataFrame
}
