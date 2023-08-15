package com.microsoft.azure.adf.dataflow.constant

/**
 * list of templates
 */
object DataFlowCodeTemplate {

  type TemplatePath = String
  // pyspark code template
  val PYSPARKCODE_TEMPLATE: TemplatePath = "code/filetemplate/PySparkTemplate"
  //scala spark code template
  val SCALASPARK_TEMLATE: TemplatePath = "code/filetemplate/SparkScalaTemplate"
  type MetaDataPath = String
  // pyspark code notebook metadata
  val PYSPARK_NOTEBOOK_METADATA: MetaDataPath = "code/notebookmetadata/PySparkSynapse"
  //scala spark notebook metadata
  val SCALASPARK_NOTEBOOK_METADATA: TemplatePath = "code/notebookmetadata/SparkScalaSynapse"
}
