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
      Try {
        val rg: String = cliAppInputArgs.getOrElse("rg", throw new InvalidArgumentException("resource group is missing"))
        val factoryName = cliAppInputArgs.getOrElse("factoryName", throw new InvalidArgumentException("factory name is missing"))
        val dataFlowName = cliAppInputArgs.getOrElse("dataFlowName", throw new InvalidArgumentException("data flow name is missing"))
        val profile = new AzureProfile(AzureEnvironment.AZURE)
        val credential: DefaultAzureCredential = new DefaultAzureCredentialBuilder().authorityHost(profile.getEnvironment.getActiveDirectoryEndpoint).build
        val manager: DataFactoryManager = DataFactoryManager.authenticate(credential, profile)
        val response: Response[DataFlowResource] = manager
          .dataFlows()
          .getWithResponse(rg, factoryName, dataFlowName, null, com.azure.core.util.Context.NONE)
        if (response.getStatusCode == 200) {
          val prop: MappingDataFlow = response.getValue.innerModel().properties().asInstanceOf[MappingDataFlow]
          val codeLines: List[String] = prop.scriptLines().asScala.toList
          Some(MdfSource(codeLines, cliAppInputArgs))
        } else {
          logger.error(s"Dataflow REST API error ${response.getStatusCode}")
          None
        }
      }.getOrElse(None)
    } else {
      None
    }
  }
}

