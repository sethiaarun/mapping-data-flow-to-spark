package com.microsoft.azure.adf.exception

/**
 * parser exception
 * @param ex
 * @param data
 */
case class ParserException(ex: Throwable, data: String) {

  override def toString: String = s"""{"exception":"${ex.toString()},"data":${data}}"""
}
