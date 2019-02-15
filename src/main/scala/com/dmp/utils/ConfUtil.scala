package com.dmp.utils

import java.util.Properties

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/2/1 10:22
  *
  */
object ConfUtil {

  private val inputStream= this.getClass.getResourceAsStream("/application.properties")
  val prop = new Properties();
  prop.load(inputStream)

  def getString(key: String) = prop.getProperty(key)

  def getInt(key: String) = getString(key).toInt

  def getBoolean(key: String) = getString(key).toBoolean

  def getLong(key: String) = getString(key).toLong

}
