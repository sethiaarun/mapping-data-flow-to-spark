package com.microsoft.azure.adf.dataflow.model.sort

object SortType extends Enumeration {
  type SortType = Value
  // Assigning values
  val DESC = Value("desc")
  val ASC = Value("asc")

  /**
   * get sort type for given input string like desc or asc
   *
   * @param str
   * @return
   */
  def getType(str: String): SortType = {
    SortType.values.find(_.toString.equalsIgnoreCase(str)).getOrElse(DESC)
  }
}
