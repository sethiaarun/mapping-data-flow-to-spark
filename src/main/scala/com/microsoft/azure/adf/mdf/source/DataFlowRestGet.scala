package com.microsoft.azure.adf.mdf.source

import com.azure.core.http.rest.Response
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.{DefaultAzureCredential, DefaultAzureCredentialBuilder}
import com.azure.resourcemanager.datafactory.DataFactoryManager
import com.azure.resourcemanager.datafactory.models.{DataFlowResource, MappingDataFlow}
import com.microsoft.azure.adf.exception.InvalidArgumentException

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

class DataFlowRestGet extends MdfScriptCodeSource {


  val SOURCE_TYPE = "API"

  /**
   * list of required arguments for given source
   *
   * @return
   */
  override def REQUIRED_ARGUMENTS: Set[ApplicationArgument] = Set(ApplicationArgument("rg", "resource group name"),
    ApplicationArgument("factoryName", "factory name"),
    ApplicationArgument("dataFlowName", "data flow name"),
    ApplicationArgument("className", "spark class name"),
    ApplicationArgument("appName", "spark application name"))


  /**
   * get script code lines
   *
   * @param cliAppInputArgs cli application input arguments
   * @return
   */
  @throws(classOf[InvalidArgumentException])
  override def scriptCodeLines(cliAppInputArgs: Map[String, String]): Option[MdfSource] = {
    if (validateFlowArguments(cliAppInputArgs)) {
      // once we have all required arguments to process the script code
      // we should add optional arguments
      val appArguments: Map[String, String] = addOptionalArguments(cliAppInputArgs)
      Try {
        val response: Response[DataFlowResource] = callDataFlowGetAPI(appArguments)
        if (response.getStatusCode == 200) {
          val prop: MappingDataFlow = response.getValue.innerModel().properties().asInstanceOf[MappingDataFlow]
          val codeLines: List[String] = prop.scriptLines().asScala.toList
          // It is going to return list of line codes and complete app arguments
          // including optional parameters
          Some(MdfSource(codeLines, appArguments))
        } else {
          logger.error(s"Dataflow REST API error ${response.getStatusCode}")
          None
        }
      }.getOrElse(None)
    } else {
      None
    }
  }

  /**
   * call Data flow REST GET API to get Dataflow script code
   *
   * @param appArguments
   */
  @throws(classOf[InvalidArgumentException])
  private def callDataFlowGetAPI(appArguments: Map[String, String]): Response[DataFlowResource] = {
    val rg = appArguments.get("rg")
    val factoryName = appArguments.get("factoryName")
    val dataFlowName = appArguments.get("dataFlowName")
    val profile = new AzureProfile(AzureEnvironment.AZURE)
    (rg, factoryName, dataFlowName) match {
      case (Some(resourceGroup), Some(factName), Some(dfName)) =>
        val credential: DefaultAzureCredential = new DefaultAzureCredentialBuilder().authorityHost(profile.getEnvironment.getActiveDirectoryEndpoint).build
        val manager: DataFactoryManager = DataFactoryManager.authenticate(credential, profile)
        manager
          .dataFlows()
          .getWithResponse(resourceGroup, factName, dfName, null, com.azure.core.util.Context.NONE)
      case _ =>
        logger.info(s"resourceGroup:${rg}, factoryName:${factoryName}, dataFlowName:${dataFlowName}")
        throw new InvalidArgumentException("required parameters are missing for the API call")
    }
  }
}

