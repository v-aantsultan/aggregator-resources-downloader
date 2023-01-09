package com.eci.common.config

import com.google.common.base.MoreObjects
import com.typesafe.config.Config

import javax.inject.Inject

class DestinationConfig @Inject()(conf: Config) {
  import DestinationConfig._

  val schema: String = conf.getString(SchemaKey)
  val table: String = conf.getString(TableKey)
  val path: String = conf.getString(PathKey)

  require(schema.nonEmpty, s"$SchemaKey $NonEmptyMessage")
  require(table.nonEmpty, s"$TableKey $NonEmptyMessage")
  require(path.nonEmpty, s"$PathKey $NonEmptyMessage")

  override def toString: String = MoreObjects.toStringHelper(this)
    .add("schema", schema)
    .add("table", table)
    .add("path", path)
    .toString
}

object DestinationConfig {
  private val SchemaKey = "schema-name"
  private val TableKey = "table-name"
  private val PathKey = "aggregator-dest"

  private val NonEmptyMessage = "has to be non empty"
}