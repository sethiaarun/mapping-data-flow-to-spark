package com.microsoft.azure.adf.dataflow.source

import com.microsoft.azure.adf.app.model.FileSourceInputArguments

import scala.io.Source

/**
 * MDF Script Code Source from file
 */
class ScriptCodeFileSource extends MdfScriptCodeSource {

  val SOURCE_TYPE = SourceTypes.FILE

  type E = FileSourceInputArguments

  /**
   * get script code lines and parsed arguments
   *
   * @param inputArgument cli application input argument source
   * @return
   */
  override def scriptCodeLines(inputArgument: FileSourceInputArguments): Option[MdfSource] = {
    val scriptFlowCode = Source.fromFile(inputArgument.inputPath)
    // It is going to return list of script line codes
    Some(MdfSource(scriptFlowCode.getLines().toList))
  }
}
