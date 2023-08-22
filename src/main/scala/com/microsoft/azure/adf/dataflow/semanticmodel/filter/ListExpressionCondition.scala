package com.microsoft.azure.adf.dataflow.semanticmodel.filter

/**
 * List of [[ExpressionCondition]] - Does not support spark code generation by itself
 *
 * @param list
 */
case class ListExpressionCondition(list: List[ExpressionCondition])