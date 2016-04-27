package com.asto.dmp.jdlp.base

import com.asto.dmp.jdlp.mq.MQAgent
import com.asto.dmp.jdlp.service.impl.MainService
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    checkArgs(args)
    new MainService().run()
    closeResources()
    logInfo((s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  def checkArgs(args: Array[String]) = {
    if (args.length != 3) {
      logError("请传入jdName,filePath,其他属性")
      sys.exit(1)
    } else {
      logInfo("args(0):" + args(0))
      logInfo("args(1):" + args(1))
      logInfo("args(2):" + args(2))

      val infoMap = args(2).split(",").map{ t =>
        val a = t.split(":")
        if(a.length == 1) (a(0),"") else (a(0),a(1))
      }.toMap
      println(infoMap)
      Constants.INPUT_FILE_PATH = args(1)
      Constants.ShopInfo.JD_NAME = args(0)
      Constants.ShopInfo.SHOP_NAME = infoMap("shopName")
      Constants.ShopInfo.USER_NAME = infoMap("userName")
      Constants.ShopInfo.SHOP_LEVEL = infoMap("shopLevel")
      Constants.ShopInfo.COMPANY_NAME = infoMap("companyName")
      Constants.ShopInfo.MAJOR_BUSINESS = infoMap("majorBusiness")
    }
  }

  private def closeResources() = {
    MQAgent.close()
    Contexts.stopSparkContext()
  }
}