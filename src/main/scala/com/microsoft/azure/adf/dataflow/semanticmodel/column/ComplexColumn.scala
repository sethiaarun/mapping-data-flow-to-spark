package com.microsoft.azure.adf.dataflow.semanticmodel.column

/**
 * Complex Column definition
 * @param name
 * @param list
 */
case class ComplexColumn(name: String, list: List[Column]) extends Column
