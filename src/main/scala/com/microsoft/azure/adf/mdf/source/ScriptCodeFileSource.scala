package com.microsoft.azure.adf.mdf.source

import com.microsoft.azure.adf.exception.InvalidArgumentException

import scala.io.Source

/**
 * MDF Script Code Source from file
 */
class ScriptCodeFileSource extends MdfScriptCodeSource {

  val SOURCE_TYPE = "File"

  /**
   * list of required arguments for given source
   *
   * @return
   */
  override def REQUIRED_ARGUMENTS: Set[ApplicationArgument] = Set(ApplicationArgument("inputpath", " script code input path"),
    ApplicationArgument("className", "spark class name"),
    ApplicationArgument("appName", "spark application name"))


  /**
   * get script code lines
   * @param cliAppInputArgs cli application input arguments
   * @return
   */
  @throws(classOf[InvalidArgumentException])
  override def scriptCodeLines(cliAppInputArgs: Map[String, String]): Option[MdfSource] = {
    if (validateFlowArguments(cliAppInputArgs)) {
      cliAppInputArgs.get("inputpath") match {
        case Some(path) => {
          // read mapping dataflow script code
          val scriptFlowCode = Source.fromFile(path)
          // list of line codes from the script code
          Some(MdfSource(scriptFlowCode.getLines().toList, cliAppInputArgs))
        }
        case _ => None
      }
    } else {
      None
    }
  }
}
