package com.microsoft.azure.adf.mdf.source

import java.util.ServiceLoader
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * List of registered MDF Script code source
 */
trait MdfCodeSourceRegistry {

  // service loader to load mdf source code sources
  protected val parserFactory: ServiceLoader[MdfScriptCodeSource] = ServiceLoader.load(classOf[MdfScriptCodeSource],
    this.getClass.getClassLoader)

  private val listOfSourceTypes: Map[String, MdfScriptCodeSource] =
    parserFactory.asScala.map(parserProvider => parserProvider.SOURCE_TYPE -> parserProvider).toMap

  /**
   * list of source types
   */
  lazy val listOfValues: List[MdfScriptCodeSource] = listOfSourceTypes.values.toList

  /**
   * get matched source type object
   *
   * @param sourceType
   * @return
   */
  def getSourceType(sourceType: String): Option[MdfScriptCodeSource] = {
    listOfSourceTypes.filter(_._1.equalsIgnoreCase(sourceType)).headOption match {
      case Some((sT, sourceObj)) => Some(sourceObj)
      case _ => None
    }
  }

}
