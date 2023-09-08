package com.microsoft.azure.adf.dataflow.semanticmodel.sort

import com.microsoft.azure.adf.dataflow.semanticmodel.sort.SortType.SortType

/**
 *
 * @param sortDirection
 * @param fieldName
 * @param nullFirst
 */
case class SortField(sortDirection: SortType, fieldName: String,nullFirst:Boolean)