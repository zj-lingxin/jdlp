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
    if (args.length != 2) {
      logError("请传入JDName、filePath")
      sys.exit(1)
    } else {
      Constants.JD_NAME = args(0)
      Constants.PATH = args(1)
      logInfo(s"JDName:${Constants.JD_NAME};Path:${Constants.PATH}")
    }
  }

  private def closeResources() = {
    MQAgent.close()
    Contexts.stopSparkContext()
  }
}