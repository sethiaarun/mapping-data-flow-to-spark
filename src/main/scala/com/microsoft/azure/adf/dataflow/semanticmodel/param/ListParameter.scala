package com.microsoft.azure.adf.dataflow.semanticmodel.param

import com.microsoft.azure.adf.dataflow.semanticmodel.{Expr, SparkCodeGenerator}


/**
 * This semantic model class provides Parameters defined for the dataflow
 *
 * @param list
 */
case class ListParameter(list: List[Parameter]) extends Expr with SparkCodeGenerator {
  override def scalaSparkCode(): String = {
    list.map(param=>{
      param.dataType.toUpperCase match {
        case "INTEGER"=>
          s"""val ${param.paramName}=${param.value}"""
        case _ =>
          s"""val ${param.paramName}="${param.value}""""
      }
    }).mkString("\n")
  }

  override def pySparkCode(): String = {
    list.map(param=>{
      param.dataType.toUpperCase match {
        case "INTEGER"=>
          s"""${param.paramName}=${param.value}"""
        case _ =>
          s"""${param.paramName}="${param.value}""""
      }
    }).mkString("\n")
  }
}
