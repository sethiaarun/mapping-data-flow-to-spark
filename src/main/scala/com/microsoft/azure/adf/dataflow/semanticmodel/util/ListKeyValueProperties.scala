package com.microsoft.azure.adf.dataflow.semanticmodel.util

import com.microsoft.azure.adf.dataflow.`type`.LanguageType.{Language, PYTHON, SCALA}

/**
 * List of Key Value Properties
 *
 * @param listKeyValueProperties
 */
case class ListKeyValueProperties(listKeyValueProperties: List[KeyValueProperties]) {

  lazy val mapKVProp: Map[String, KeyValueProperties] = listKeyValueProperties.map(kv => (kv.propName -> kv)).toMap

  /**
   * given key name find KV Prop object
   *
   * @param keyName
   * @param defaultValue
   * @param lang
   * @return
   */
  def getOrElse(keyName: String, defaultValue: String, lang: Language): String = {
    get(keyName, lang) match {
      case Some(v) => v
      case _ => defaultValue
    }
  }

  /**
   * given key name find KV Prop object
   *
   * @param keyName
   * @param lang
   * @return
   */
  def get(keyName: String, lang: Language): Option[String] = {
    mapKVProp.get(keyName) match {
      case Some(kv) =>
        lang match {
          case SCALA => Some(kv.propValue)
          case PYTHON =>
            // if the value is parameter value means it will be replaced at runtime
            // then we should remove $ sign from value and add open and close curly braces to wrap
            Some(
              if (kv.isParamVariable && kv.propValue.startsWith("$"))
                "{" + kv.propValue.replaceFirst("\\$", "") + "}"
              else kv.propValue
            )
        }
      case _ => None
    }
  }
}
