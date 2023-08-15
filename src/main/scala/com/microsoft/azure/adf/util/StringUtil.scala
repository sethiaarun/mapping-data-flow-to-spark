package com.microsoft.azure.adf.util

import scala.annotation.tailrec

/**
 * String util functions
 */
object StringUtil {


  /**
   * add brackets for given list of string elements
   * Input: List("1","2","3")
   * Output: 1(2(3))
   * Input: List("3")
   * Output: 3
   * Input: List("1","2")
   * Output: 1(2)
   *
   * @param list
   * @return
   */
  def addBrackets(list: List[String]): String = {
    val result = _addBrackets(list.reverse, 0, Nil)
    result.mkString("")
  }

  /**
   * Concatenate List of String by given string
   * @param concatBy
   * @param list
   * @return
   */
  def concatString(concatBy:String, list: Option[String]*) = list.flatten.mkString(concatBy)
  
  /**
   * tail recursion for add brackets
   * @param list
   * @param numOfOpenBrackets
   * @param acc
   * @return
   */
  @tailrec
  private def _addBrackets(list: List[String], numOfOpenBrackets: Int, acc: List[String]): List[String] = {
    if (list.isEmpty) {
      if (numOfOpenBrackets > 0) {
        val closeBrackets = List.fill(numOfOpenBrackets)(")")
        (acc ::: closeBrackets)
      } else {
        acc
      }
    } else {
      list match {
        case h :: t =>
          if (t.isEmpty) {
            _addBrackets(t, numOfOpenBrackets, h :: acc)
          } else {
            _addBrackets(t, numOfOpenBrackets + 1, "(" + h :: acc)
          }
        case Nil =>
          _addBrackets(list.tail, numOfOpenBrackets, acc)
      }
    }
  }
}
