package com.microsoft.azure.adf.dataflow.source

import com.azure.core.http.rest.Response
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.{DefaultAzureCredential, DefaultAzureCredentialBuilder}
import com.azure.resourcemanager.datafactory.DataFactoryManager
import com.azure.resourcemanager.datafactory.models.{DataFlowResource, MappingDataFlow}
import com.microsoft.azure.adf.app.model.RestGetSourceInputArguments

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

class DataFlowRestGet extends MdfScriptCodeSource {


  val SOURCE_TYPE = SourceTypes.API

  type E = RestGetSourceInputArguments
  /**
   * get script code lines
   *
   * @param cliAppInputArgs cli application input arguments
   * @return
   */
  override def scriptCodeLines(inputArgument: RestGetSourceInputArguments): Option[MdfSource] = {
    Try {
      val response: Response[DataFlowResource] = callDataFlowGetAPI(inputArgument)
      if (response.getStatusCode == 200) {
        val prop: MappingDataFlow = response.getValue.innerModel().properties().asInstanceOf[MappingDataFlow]
        val codeLines: List[String] = prop.scriptLines().asScala.toList
        // It is going to return list of script line codes
        Some(MdfSource(codeLines))
      } else {
        logger.error(s"Dataflow REST API error ${response.getStatusCode}")
        None
      }
    }.getOrElse(None)
  }

  /**
   * call Data flow REST GET API to get Dataflow script code
   *
   * @param appArguments
   */
  private def callDataFlowGetAPI(inputArgument: RestGetSourceInputArguments): Response[DataFlowResource] = {
    val profile = new AzureProfile(AzureEnvironment.AZURE)
    val credential: DefaultAzureCredential = new DefaultAzureCredentialBuilder().authorityHost(profile.getEnvironment.getActiveDirectoryEndpoint).build
    val manager: DataFactoryManager = DataFactoryManager.authenticate(credential, profile)
    manager
      .dataFlows()
      .getWithResponse(inputArgument.rg,
        inputArgument.factoryName,
        inputArgument.dataFlowName,
        null,
        com.azure.core.util.Context.NONE
      )
  }
}

