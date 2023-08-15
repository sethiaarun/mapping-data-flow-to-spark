package com.microsoft.azure.adf.dataflow.model.source

/**
 * list of column name and type - Does not support spark code generation by itself
 *
 * @param list
 */
case class ColumnDefinitionList(list: List[ColumnDefinition])
