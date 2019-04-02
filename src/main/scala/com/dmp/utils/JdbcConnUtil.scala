package com.dmp.utils

import java.util.Properties

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/4/2 14:07
  *
  */
object JdbcConnUtil {

  def getConn(tableName: String): (String, String, Properties) = {

    val url: String = ConfUtil.getString("jdbc.url")
    val table: String = ConfUtil.getString(tableName)
    val user = ConfUtil.getString("jdbc.user")
    val password = ConfUtil.getString("jdbc.password")
    val connProp = new Properties()
    connProp.put("user", user)
    connProp.put("password", password)
    (url, table, connProp)
  }


}



