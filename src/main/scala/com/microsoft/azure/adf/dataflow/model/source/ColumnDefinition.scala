package com.microsoft.azure.adf.dataflow.model.source

/**
 * Column Name and Type - Does not support spark code generation by itself
 *
 * @param name   name of the column
 * @param f_type data type
 */
case class ColumnDefinition(name: String, f_type: String)
