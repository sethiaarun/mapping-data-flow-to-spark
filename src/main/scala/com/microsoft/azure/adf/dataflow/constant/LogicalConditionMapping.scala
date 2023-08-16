package com.microsoft.azure.adf.dataflow.constant

/**
 * this object provides logical condition mapping between
 * DataFlow and Sala Spark and DataFlow and PySpark
 */
object LogicalConditionMapping {

  // mapping for Scala Spark
  val DATAFLOW_LOGICAL_COND_MAP_SCALA = Map("&&" -> "&&", "||" -> "||")

  //mapping for PySpark
  val DATAFLOW_LOGICAL_COND_MAP_PY = Map("&&" -> "&", "||" -> "|")

}
