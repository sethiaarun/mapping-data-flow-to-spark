package com.microsoft.azure.adf.app

import com.microsoft.azure.adf.app.model.{InputArgumentFactory, InputArguments}

import scala.collection.mutable

/**
 * Application argument parser
 */
trait ApplicationArgumentParser {


  /**
   * parse application arguments and create required application argument model
   *
   * @param args
   * @return
   */
  def parseAppArg(args: Array[String]): InputArguments = {
    val argTemplate: mutable.Builder[(String, String), Map[String, String]] = Map.newBuilder[String, String]
    val argGrouped = args.map(arg => arg.replace("--", "")).grouped(2)
    argGrouped.foreach(arg => argTemplate.+=(arg(0) -> arg(1)))
    val argMap = argTemplate.result()
    InputArgumentFactory.build(argMap)
  }

}
