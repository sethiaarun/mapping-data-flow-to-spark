package com.microsoft.azure.adf.dataflow.constant

/**
 * list of templates
 * TODO: In future we can read these meta data template using Template reader and apply arguments
 * like Microsoft Fabric WorkSpace, Lakehouse, etc.
 */
object DataFlowCodeTemplate {

  type TemplatePath = String

  // pyspark code template
  val PYSPARKCODE_TEMPLATE: TemplatePath = "code/filetemplate/PySparkTemplate"
  //scala spark code template
  val SCALASPARK_TEMLATE: TemplatePath = "code/filetemplate/SparkScalaTemplate"
  type MetaDataPath = String
  // pyspark fabric notebook metadata
  val PYSPARK_FABRIC_NOTEBOOK_METADATA: MetaDataPath = "code/notebookmetadata/PySparkFabric"
  //scala spark fabric notebook metadata
  val SCALASPARK_FABRIC_NOTEBOOK_METADATA: TemplatePath = "code/notebookmetadata/SparkScalaFabric"
}
