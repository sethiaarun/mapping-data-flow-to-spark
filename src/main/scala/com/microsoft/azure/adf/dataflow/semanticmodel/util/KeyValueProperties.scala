package com.microsoft.azure.adf.dataflow.semanticmodel.util

/**
 * key and value properties
 *
 * @param propName
 * @param propValue
 * @param isParamVariable
 */
case class KeyValueProperties(propName: String, propValue: String, isParamVariable: Boolean = false)
