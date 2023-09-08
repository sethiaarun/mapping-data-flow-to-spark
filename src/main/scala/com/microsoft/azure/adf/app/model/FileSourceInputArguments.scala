package com.microsoft.azure.adf.app.model

import com.microsoft.azure.adf.dataflow.source.SourceTypes
import com.microsoft.azure.adf.dataflow.source.SourceTypes.SourceType

/**
 * Model class mapping from application arguments when source is file
 *
 * @param inputPath
 * @param className
 * @param appName
 */
case class FileSourceInputArguments(inputPath: String,
                                    className: String,
                                    appName: String,
                                    fabricRunTimeEnv: FabricRunTimeEnv) extends InputArguments {
  val sourceType:SourceType = SourceTypes.FILE
  require(!inputPath.isEmpty, "Input file path is missing, file with json representation of script code")
  require(!className.isEmpty, "class name is missing, output spark scala file name")
  require(!appName.isEmpty, "class name is missing, output spark scala file name")
}

/**
 * File Input source semantic model builder
 */
object FileSourceInputArgument {
  def apply(args: Map[String, String]): FileSourceInputArguments = {
    FileSourceInputArguments(args.getOrElse("inputPath", ""),
      args.getOrElse("className", ""),
      args.getOrElse("appName", ""),
      FabricRunTimeEnv.apply(args)
    )
  }
}