package com.microsoft.azure.adf.app.model

import com.microsoft.azure.adf.dataflow.source.SourceTypes
import com.microsoft.azure.adf.dataflow.source.SourceTypes.SourceType

/**
 * Model class mapping from application arguments when source is REST GET from dataflow API
 *
 * @param rg
 * @param factoryName
 * @param dataFlowName
 * @param className
 * @param appName
 * @param fabricRunTimeEnv
 */
case class RestGetSourceInputArguments(rg: String,
                                       factoryName: String,
                                       dataFlowName: String,
                                       className: String,
                                       appName: String,
                                       fabricRunTimeEnv: FabricRunTimeEnv) extends InputArguments {
  val sourceType:SourceType = SourceTypes.API
  require(!rg.isEmpty, "Dataflow resource group name is missing")
  require(!factoryName.isEmpty, "Azure data factory name is missing")
  require(!dataFlowName.isEmpty, "Dataflow name is missing")
  require(!className.isEmpty, "class name is missing, output spark scala file name")
  require(!appName.isEmpty, "class name is missing, output spark scala file name")
}


/**
 * REST Get Input Argument model builder
 */
object RestGetSourceInputArgument {

  def apply(args: Map[String, String]): RestGetSourceInputArguments = {
    RestGetSourceInputArguments(args.getOrElse("rg", ""),
      args.getOrElse("factoryName", ""),
      args.getOrElse("dataFlowName", ""),
      args.getOrElse("className", ""),
      args.getOrElse("appName", ""),
      FabricRunTimeEnv.apply(args)
    )
  }
}

