package com.asto.dmp.jdlp.service.impl

import com.asto.dmp.jdlp.base.{Constants, Props, CsvDF}
import com.asto.dmp.jdlp.cos.FileUploader
import com.asto.dmp.jdlp.mq.MQAgent
import com.asto.dmp.jdlp.service.{JDLPWorkBook, Service}
import com.asto.dmp.jdlp.util.Utils
import org.json.JSONObject

class MainService extends Service {
  private val filePath = (fileName: String) => {
    s"file://${Constants.PATH}/$fileName"
  }

  val dynamic = CsvDF.load(filePath(Props.get("dynamic_file")))
  val detail = CsvDF.load(filePath(Props.get("detail_file")))
  val refund = CsvDF.load(filePath(Props.get("refund_file")))
  val summary = CsvDF.load(filePath(Props.get("summary_file")))
  val traffic = CsvDF.load(filePath(Props.get("traffic_file")))

  def getDSR = {
    val toPercent = (value: Any) => {
      Utils.retainDecimal(value.toString.toDouble * 100).toString
    }
    dynamic.select("商品描述满意度", "行业相比【描】", "服务态度满意度", "行业相比【服】", "物流速度满意度", "行业相比【物】")
      .map(a => Array(Utils.retainDecimal(a(0).toString.toDouble).toString, toPercent(a(1)), Utils.retainDecimal(a(2).toString.toDouble).toString, toPercent(a(3)), Utils.retainDecimal(a(4).toString.toDouble).toString, toPercent(a(5)))).collect()(0)

  }

  /**
   * 上月的退款额除以上月的下单额
   */
  def getRefundRate = {
    val (time, refundAmount) = refund.select("当前月", "退货金额").map(a => (a(0).toString, a(1).toString.toDouble)).collect()(0)
    val saleAmount = summary.select("当前月", "下单金额").map(a => (a(0).toString, a(1).toString.toDouble)).filter(_._1 == time).map(_._2).collect()(0)
    Utils.retainDecimal(refundAmount / saleAmount * 100).toString
  }

  def getSaleInfo = {
    val info = summary.select("当前月", "下单金额", "浏览量", "访客数", "店铺成交转化率", "客单价")
      .map(a => (a(0).toString, (a(1).toString.toDouble, a(2).toString.toInt, a(3).toString.toInt, a(4).toString.toDouble, a(5).toString.toDouble)))
    val flowRate = traffic.select("当前月", "流量占比").map(a => (a(0).toString, a(1).toString.toDouble))
    val top3Amount = detail.select("当前月", "下单金额").map(a => (a(0).toString, a(1).toString.toDouble)).reduceByKey(_ + _)
    info.cogroup(flowRate, top3Amount).map(t => (t._1.toString, t._2._1.head, t._2._2.head, t._2._3.head))
      .map(t => (t._1, t._2._1.toInt, t._2._2, t._2._3, Utils.retainDecimal(t._2._4 * 100), Utils.retainDecimal(t._2._5), Utils.retainDecimal(t._3 * 100), "", Utils.retainDecimal(t._4 / t._2._1 * 100)))
      .sortBy(_._1, ascending = false).persist()
  }

  def getSaleInfoOfLast3M = {
    getSaleInfo.top(3).sortBy(_._1)
  }

  def avgSaleInfo(saleInfo: Array[(String, Int, Int, Int, Double, Double, Double, String, Double)]): Array[String] = {
    val num = saleInfo.length
    val sum = saleInfo.reduce((a, b) => ("均值", a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9))
    Array(
      "均值",
      Utils.retainDecimal(sum._2 / num, 0).toInt.toString,
      Utils.retainDecimal(sum._3 / num, 0).toInt.toString,
      Utils.retainDecimal(sum._4 / num, 0).toInt.toString,
      Utils.retainDecimal(sum._5 / num, 2) + "%",
      Utils.retainDecimal(sum._6 / num, 2).toString,
      Utils.retainDecimal(sum._7 / num, 2) + "%",
      "",
      Utils.retainDecimal(sum._9 / num, 2) + "%"
    )
  }

  def getSaleInfoAYear = {
    getSaleInfo.top(12).sortBy(_._1)
  }

  def toStringArray(array: Array[(String, Int, Int, Int, Double, Double, Double, String, Double)]): Array[Array[String]] = {
    array.map(t => Array(
      t._1,
      t._2.toString,
      t._3.toString,
      t._4.toString,
      t._5.toString + "%",
      t._6.toString,
      t._7.toString + "%",
      t._8.toString,
      t._9.toString + "%"
    ))
  }

  override private[service] def runServices(): Unit = {

    val workBook = new JDLPWorkBook
    val dsr = getDSR
    val refundRate = getRefundRate
    val saleInfoAYear = getSaleInfoAYear
    val avgSaleInfoAYear = avgSaleInfo(saleInfoAYear)
    val saleInfo3M = getSaleInfoOfLast3M
    val avgSaleInfo3M = avgSaleInfo(saleInfo3M)

    workBook.setContents(dsr, refundRate, toStringArray(saleInfo3M), avgSaleInfo3M, toStringArray(saleInfoAYear), avgSaleInfoAYear)

    val fileName = s"${Constants.JD_NAME}.xls"
    val localPath = s"${Constants.PATH}/$fileName"
    //将文件保存到本地
    workBook.saveToLocal(localPath)
    var fileUrl = ""
    var times = 1
    do {
      //将文件上传到cos
      logInfo(s"尝试将文件上传到cos：第${times}次")
      if(times!=1) Thread.sleep(20000)
      fileUrl = new FileUploader().upload(fileName, localPath)
      times = times + 1
    } while (fileUrl == "" && times < 20)
    //发送MQ消息
    val jsonObject = new JSONObject().put("jdName", Constants.JD_NAME).put("fileUrl", fileUrl).put("success", if (fileUrl == "") false else true)
    logInfo("向MQ发送消息：" + jsonObject.toString())
    MQAgent.send(jsonObject.toString())
  }
}
