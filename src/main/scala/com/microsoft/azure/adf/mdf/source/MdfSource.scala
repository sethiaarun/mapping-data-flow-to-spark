package com.microsoft.azure.adf.mdf.source

/**
 * this object is created once the source is able to read the script code and
 * all required arguments are present from the application cli
 *
 * @param listOfCodeLines
 * @param parsedArgs
 */
case class MdfSource(listOfCodeLines: List[String], parsedArgs: Map[String, String])
