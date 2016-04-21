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
      // arg(0) "jdName":"1元宝"
      // arg(1) "path":"/jdlp/xxtest.xls"
      // arg(2) "shopName":"店铺名称","userName":"借款人名称","shopLevel":"店铺等级","companyName":"法人名称","majorBusiness":"网店主营业务"
      logInfo("args(0):" + args(0))
      logInfo("args(1):" + args(1))
      logInfo("args(2):" + args(2))
      val info = args(2).split(",")

      Constants.INPUT_FILE_PATH = args(1)
      Constants.ShopInfo.JD_NAME = args(0)
      Constants.ShopInfo.SHOP_NAME = info(0)
      Constants.ShopInfo.USER_NAME = info(1)
      Constants.ShopInfo.SHOP_LEVEL = info(2)
      Constants.ShopInfo.COMPANY_NAME = info(3)
      Constants.ShopInfo.MAJOR_BUSINESS = info(4)
    }
  }

  private def closeResources() = {
    MQAgent.close()
    Contexts.stopSparkContext()
  }
}