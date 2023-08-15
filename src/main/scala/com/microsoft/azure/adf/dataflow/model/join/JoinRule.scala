package com.microsoft.azure.adf.dataflow.model.join

import com.microsoft.azure.adf.dataflow.model.filter.ListExpressionCondition
import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties

/**
 * Join rule definition - Does not support spark code generation by itself

 * @param joinProp join properties
 */
case class JoinRule(lstExpCondition: ListExpressionCondition, joinProp:ListKeyValueProperties)