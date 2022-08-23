package checks

import org.scalatest.flatspec.AnyFlatSpec

class DataQualityTest extends AnyFlatSpec{
  "null check " should " check for nulls in the final DF " in {
    val inputDF = Seq (
      ("D0694","41810","2020-11-15 19:29:00","android","B001597", "C5492","facebook","false","true", "22.0","I351","Clothing & Accessories","2","AMBER PRODUCTS","2020-11-15","2022-08-23 01:12:12"),
      |  I0429|32042|2020-11-15 15:56:00|    android|   B006990|      E223|            google|         false|           true|    1382.0|        H969|        Apps & Games|        4| LARVEL SUPPLY|2020-11-15|2022-08-23 01:12:12|
      |  F2957|42261|2020-11-15 19:35:00|    android|   B008223|     D9702|           youtube|          true|          false|    2275.0|        J943|                Baby|        2|AMBER PRODUCTS|2020-11-15|2022-08-23 01:12:12|
      |  I1394|43060|2020-11-15 19:47:00|    android|   B009250|     B9232|            google|         false|           true|   13248.0|        C874|              Beauty|        1|     KOROL LLC|2020-11-15|2022-08-23 01:12:12|
      |  B6959|27933|2020-11-15 14:25:00|    android|   B009397|     F6622|          linkedin|         false|           true|    1449.5|          G0|        Collectibles|        2|AMBER PRODUCTS|2020-11-15|2022-08-23 01:12:12|
    )

  }

}
