package com.microsoft.azure.adf.mdf.source

import com.microsoft.azure.adf.exception.InvalidArgumentException
import com.typesafe.scalalogging.Logger

/**
 * Mapping dataflow Script code source
 * It is implemented by two different sources
 * 1. File - [[ScriptCodeFileSource]]
 * 2. REST API [[DataFlowRestGet]]
 */
trait MdfScriptCodeSource {

  protected val logger = Logger(getClass.getName)

  //name of the source
  val SOURCE_TYPE: String


  /**
   * list of required arguments for given source
   *
   * @return
   */
  protected def REQUIRED_ARGUMENTS: Set[ApplicationArgument]

  /**
   * list of optional arguments for given source
   *
   * @return
   */
  protected def OPTIONAL_ARGUMENTS: Set[ApplicationArgument] = Set(
    ApplicationArgument("lakeHouseId", "Microsoft Fabric lakehouseId"),
    ApplicationArgument("lakeHouseName", "Microsoft Fabric lakehouse name"),
    ApplicationArgument("workSpaceId", "Microsoft Fabric workSpaceId"),
  )

  /**
   * get script code lines and parsed arguments
   *
   * @param cliAppInputArgs cli application input arguments
   * @return
   */
  def scriptCodeLines(cliAppInputArgs: Map[String, String]): Option[MdfSource]

  /**
   * It is going to parse main program input arguments
   * and returns true/false, if number of input parameters are less than required parameters
   * it will fail and exit from the system
   *
   * @param cliAppInputArgs
   */
  @throws(classOf[InvalidArgumentException])
  protected def validateFlowArguments(cliAppInputArgs: Map[String, String]): Boolean = {
    val usageArgs = REQUIRED_ARGUMENTS.map(arg => s"""[--${arg.inputOption} ${arg.message}]""").mkString(" ")
    val usage = s"Usage: DataFlowExample ${usageArgs}"
    if (cliAppInputArgs.size < REQUIRED_ARGUMENTS.size) {
      println(usage)
      throw new InvalidArgumentException(s"missing required arguments ${usage}")
    } else {
      validateArgument(cliAppInputArgs)
    }
  }

  /**
   * validate if all required keys are present in parseArg
   *
   * @param parsedArg
   * @return
   */
  @throws(classOf[InvalidArgumentException])
  private def validateArgument(parsedArg: Map[String, String]): Boolean = {
    val inputArgKeys = parsedArg.keys.toSet
    val requiredParam = REQUIRED_ARGUMENTS.map(arg => arg.inputOption).toSet
    val missingArgs = requiredParam.diff(inputArgKeys)
    if (!missingArgs.isEmpty) {
      logger.error(s"Following arguments are missing ${missingArgs.mkString(",")}")
      throw new InvalidArgumentException(s"missing required arguments ${missingArgs.mkString(",")}")
    }
    true
  }

  /**
   * add optional arguments with application cli arguments
   * If arguments are pass from cli it will use the same else It will use the default value
   * @param cliAppInputArgs
   * @return
   */
  protected def addOptionalArguments(cliAppInputArgs: Map[String, String]): Map[String, String] = {
    val optionalArgs = OPTIONAL_ARGUMENTS.map(arg => {
      val name = arg.inputOption
      (name -> cliAppInputArgs.getOrElse(name, arg.defaultValue))
    }).toMap
    // add optional with cliApp args
    cliAppInputArgs ++ optionalArgs
  }
}
