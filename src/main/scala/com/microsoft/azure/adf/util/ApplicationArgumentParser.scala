package com.microsoft.azure.adf.util

import scala.collection.mutable

/**
 * Application argument parser
 */
trait ApplicationArgumentParser {
  def parseAppArg(args: Array[String]): Map[String, String] = {
    val argTemplate: mutable.Builder[(String, String), Map[String, String]] = Map.newBuilder[String, String]
    val argGrouped = args.map(arg => arg.replace("--", "")).grouped(2)
    argGrouped.foreach(arg => argTemplate.+=(arg(0) -> arg(1)))
    argTemplate.result()
  }
}
