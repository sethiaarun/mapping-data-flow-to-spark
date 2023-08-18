package com.microsoft.azure.adf.app.model

import com.microsoft.azure.adf.exception.InvalidArgumentException
import com.microsoft.azure.adf.dataflow.source.SourceTypes


/**
 * provide [[InputArguments]] object
 */
object InputArgumentFactory {

  /**
   * build
   *
   * @param sourceType
   * @param appArgs
   * @return
   */
  @throws(classOf[InvalidArgumentException])
  def build(appArgs: Map[String, String]): InputArguments = {
    appArgs.get("source") match {
      case Some(fileSource) if(fileSource.equalsIgnoreCase(SourceTypes.FILE)) =>
        FileSourceInputArgument.apply(appArgs)
      case Some(apiSource) if(apiSource.equalsIgnoreCase(SourceTypes.API)) =>
        RestGetSourceInputArgument.apply(appArgs)
      case _ =>
        throw new InvalidArgumentException(s"invalid source type")
    }
  }

}
