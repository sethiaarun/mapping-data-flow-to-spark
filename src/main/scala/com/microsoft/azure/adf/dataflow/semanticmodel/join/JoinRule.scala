package com.microsoft.azure.adf.dataflow.semanticmodel.join

import com.microsoft.azure.adf.dataflow.semanticmodel.filter.ListExpressionCondition
import com.microsoft.azure.adf.dataflow.semanticmodel.util.ListKeyValueProperties

/**
 * Join rule definition - Does not support spark code generation by itself

 * @param joinProp join properties
 */
case class JoinRule(lstExpCondition: ListExpressionCondition, joinProp:ListKeyValueProperties)