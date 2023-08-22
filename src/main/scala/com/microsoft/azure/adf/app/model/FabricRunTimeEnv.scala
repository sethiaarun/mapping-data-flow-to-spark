package com.microsoft.azure.adf.app.model

/**
 * Fabric runtime environment variables
 *
 * @param lakeHouseId
 * @param lakeHouseName
 * @param workSpaceId
 */
case class FabricRunTimeEnv(lakeHouseId: String, lakeHouseName: String, workSpaceId: String)


/**
 * Fabric runtime environment semantic model builder
 */
object FabricRunTimeEnv {
  def apply(args: Map[String, String]): FabricRunTimeEnv = {
    new FabricRunTimeEnv(args.getOrElse("lakeHouseId", ""),
      args.getOrElse("lakeHouseName", ""),
      args.getOrElse("workSpaceId", "")
    )
  }
}
