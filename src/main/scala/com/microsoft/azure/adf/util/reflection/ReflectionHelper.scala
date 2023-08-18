package com.microsoft.azure.adf.util.reflection

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

/**
 * List of function for Scala reflection
 */
object ReflectionHelper {

  def mirror(): ru.Mirror = {
    ru.runtimeMirror(getClass.getClassLoader)
  }

  /**
   * constructor parameters with index number
   *
   * @param tg
   * @tparam T
   * @return
   */
  def getConstructorParams[T]()(implicit tg: TypeTag[T]): List[(ru.Symbol, Int)] = {
    val conSymbol = constructorSymbol()
    conSymbol
      .paramLists
      .headOption
      .toList
      .flatten
      .zipWithIndex
  }


  /**
   * get constructor symbol
   *
   * @param tg
   * @tparam T
   * @return
   */
  def constructorSymbol[T]()(implicit tg: TypeTag[T]): ru.MethodSymbol = {
    val mirror = ReflectionHelper.mirror
    ru.typeOf[T].decl(ru.termNames.CONSTRUCTOR).asMethod
  }


}
