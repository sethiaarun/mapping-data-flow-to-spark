package com.microsoft.azure.adf.dataflow.semanticmodel.source

import com.microsoft.azure.adf.dataflow.semanticmodel.column.ColumnDefinitionList

/**
 * Output from the Source Flow - Does not support spark code generation by itself
 *
 * @param fieldList list of [[FieldNameTypeList]]
 */
case class DataFlowSourceOutPut(nameTypeList: ColumnDefinitionList)
