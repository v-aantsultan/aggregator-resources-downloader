package com.eci.common.services

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object JsonExtractor extends Serializable {
  def extractJson[T: Manifest](jsonString: String, path: String): Option[T] = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats: DefaultFormats.type = DefaultFormats
    try {
      val json = parse(jsonString)
      path.split('.').foldLeft(json)({ case (acc, node) => acc \ node }).extractOpt[T]
    } catch {
      case e: Exception => println(e.getMessage)
        None
    }
  }

  /**
   * Extract String value from json
   *
   * @param path defined path to get value from
   * @return
   */
  def extractString(path: String): UserDefinedFunction = udf {
    jsonString: String => extractJson[String](jsonString, path)
  }
}