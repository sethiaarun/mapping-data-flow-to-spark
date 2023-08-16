package com.microsoft.azure.adf.dataflow.constant

import com.microsoft.azure.adf.dataflow.constant.Operator._

/**
 * this object provides logical condition mapping between
 * DataFlow and Sala Spark and DataFlow and PySpark
 */
object LogicalOperatorMapping {

  // mapping for Scala Spark

  val DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA = Map(EQ -> EQ_NULL, LT_EQ -> LT_EQ, GT_EQ -> GT_EQ,
    LT_EQ -> LT_EQ, GT -> GT, EQ_NULL -> EQ_NULL, NOT_EQ -> NOT_EQ)

  // mapping for PY Spark
  val DATAFLOW_LOGICAL_OPERATOR_MAP_PY = Map(EQ -> EQ, LT_EQ -> LT_EQ, GT_EQ -> GT_EQ,
    LT_EQ -> LT_EQ, GT -> GT, EQ_NULL -> EQ, NOT_EQ -> NOT_EQ)
}
