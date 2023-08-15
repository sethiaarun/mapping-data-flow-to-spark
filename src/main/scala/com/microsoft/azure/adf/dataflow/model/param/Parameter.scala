package com.microsoft.azure.adf.dataflow.model.param

/**
 * Parameter model to store information like
 * FileName as string ("TestFile") or
 * NumOfFiles as integer (0)
 * @param paramName
 * @param dataType
 * @param value
 */
case class Parameter(paramName: String, dataType: String, value:String)
