package com.microsoft.azure.adf.dataflow.source

/**
 * List of source types
 */
object SourceTypes {

  type SourceType = String

  // file source
  val FILE: SourceType = "File"
  // REST API
  val API: SourceType = "API"
}
