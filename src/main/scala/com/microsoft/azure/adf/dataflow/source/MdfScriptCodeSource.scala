package com.microsoft.azure.adf.dataflow.source

import com.microsoft.azure.adf.app.model.InputArguments
import com.microsoft.azure.adf.dataflow.source.SourceTypes.SourceType
import com.typesafe.scalalogging.Logger

/**
 * Mapping dataflow Script code source
 * It is implemented by two different sources
 * 1. File - [[ScriptCodeFileSource]]
 * 2. REST API [[DataFlowRestGet]]
 */
trait MdfScriptCodeSource {

  protected val logger = Logger(getClass.getName)

  type E <: InputArguments

  //name of the source
  val SOURCE_TYPE: SourceType

  /**
   * get script code lines and parsed arguments
   *
   * @param inputArgument cli application input argument source
   * @return
   */
  def scriptCodeLines(inputArgument: E): Option[MdfSource]

}
