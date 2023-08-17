package com.microsoft.azure.adf.mdf.source

/**
 * Application argument
 *
 * @param inputOption  input option name for example --file test, here file is an input option
 * @param message      help message for input argument
 * @param defaultValue optional default value if not present
 */
case class ApplicationArgument(inputOption: String, message: String, defaultValue: String = "")