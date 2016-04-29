package com.asto.dmp.jdlp.service.impl

import com.asto.dmp.jdlp.base.{Constants, Props, CsvDF}
import com.asto.dmp.jdlp.cos.FileUploader
import com.asto.dmp.jdlp.mq.MQAgent
import com.asto.dmp.jdlp.service.{JDLPWorkBook, Service}
import com.asto.dmp.jdlp.util.{DateUtils, Utils}
import org.json.JSONObject
import com.fasterxml.jackson.annotation.ObjectIdGenerators.UUIDGenerator

class MainService extends Service {
  private val filePath = (fileName: String) => {
    s"file://${Constants.INPUT_FILE_PATH}/$fileName"
  }

  val dynamic = CsvDF.load(filePath(Props.get("dynamic_file")))
  val detail = CsvDF.load(filePath(Props.get("detail_file")))
  val refund = CsvDF.load(filePath(Props.get("refund_file")))
  val summary = CsvDF.load(filePath(Props.get("summary_file")))
  val traffic = CsvDF.load(filePath(Props.get("traffic_file")))

  def getDSR = {
    val toPercent = (value: Any) => {
      if (value.toString == "") ""
      else Utils.retainDecimalDown(value.toString.toDouble).toString
    }
    dynamic.select("商品描述满意度", "行业相比【描】", "服务态度满意度", "行业相比【服】", "物流速度满意度", "行业相比【物】")
      .map(a => Array(Utils.retainDecimalDown(a(0).toString.toDouble).toString, toPercent(a(1)), Utils.retainDecimalDown(a(2).toString.toDouble).toString, toPercent(a(3)), Utils.retainDecimalDown(a(4).toString.toDouble).toString, toPercent(a(5)))).collect()(0)
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
      .map(t => (t._1, t._2._1.toInt, t._2._2, t._2._3, Utils.retainDecimal(t._2._4 * 100), Utils.retainDecimal(t._2._5), Utils.retainDecimal(t._3 * 100), "", Utils.retainDecimal(if (t._2._1 == 0) 0 else (t._4 / t._2._1 * 100))))
      .sortBy(_._1, ascending = false).persist()
  }

  def getSaleInfoOfLast3M = {
    getSaleInfo.top(3).sortBy(_._1)
  }

  def avgSaleInfo(saleInfo: Array[(String, Int, Int, Int, Double, Double, Double, String, Double)]): Array[String] = {
    val num = saleInfo.length.toDouble
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

  def uploadFile(fileName: String, localPath: String) = {
    var fileUrl = ""
    var times = 1
    //有时上传会失败，所以失败时多尝试几次
    do {
      //将文件上传到cos
      logInfo(s"尝试将文件上传到cos：第${times}次")
      if (times != 1) Thread.sleep(20000)
      fileUrl = new FileUploader().upload(fileName, localPath)
      times = times + 1
    } while (fileUrl == "" && times < 20)
    fileUrl
  }

  def sendFileMsgToMQ(fileUrl: String) = {
    val jsonObject = new JSONObject().put("jdid", Constants.ShopInfo.JD_NAME).put("fileUrl", fileUrl)
    logInfo("向MQ发送消息：" + jsonObject.toString)
    MQAgent.send(jsonObject.toString, Props.get("jd_file_queue_name"))
  }

  def sendScoreAndCreditToMQ(score: String, credit: String) = {
    def getSubJson(quotaCode: String, quotaValue: String): JSONObject = {
      new JSONObject().put("indexFlag", "1")
        .put("quotaCode", quotaCode)
        .put("quotaValue", quotaValue)
        .put("targetTime", DateUtils.getStrDate("yyyyMMdd"))
    }
    val jsonObject = new JSONObject().put("jdid", Constants.ShopInfo.JD_NAME).put("quotaItemList", Array(
      getSubJson("M_PROP_CREDIT_SCORE", score),
      getSubJson("M_PROP_CREDIT_LIMIT_AMOUNT", credit)
    ))
    logInfo("向MQ发送消息：" + jsonObject.toString)
    MQAgent.send(jsonObject.toString, Props.get("jd_credit_grade_queue_name"))
  }

  def getScoreAndCreditAmount(dsr: Array[String], refundRate: String, avgSaleInfo3M: Array[String], avgSaleInfoAYear: Array[String]) = {
    //近12个月销售额均值
    val avgSaleOf12M = avgSaleInfoAYear(1).toDouble
    logInfo("近12个月销售额均值:" + avgSaleOf12M)
    //近3个月付费流量占比
    val avgFlowOf3M = avgSaleInfo3M(6).replace("%", "").toDouble * 0.01
    logInfo("近3个月付费流量占比:" + avgFlowOf3M)
    //近3个月ROI均值
    val avgRoiOf3M = avgSaleInfo3M(7)
    logInfo("近3个月ROI均值:" + avgRoiOf3M)
    //近3个月top3占比均值
    val avgTop3Of3M = avgSaleInfo3M(8).replace("%", "").toDouble * 0.01
    logInfo("近3个月top3占比均值:" + avgTop3Of3M)
    def dsrToDouble(dsr: String) = if (dsr == "") 0D else dsr.toDouble
    //dsr三项与行业对比均值
    val avgDsrComp = (dsrToDouble(dsr(1)) + dsrToDouble(dsr(3)) + dsrToDouble(dsr(5))) / 3 * 0.01
    logInfo("dsr三项与行业对比均值:" + avgDsrComp)
    //上月退款率
    val refundRateD = refundRate.toDouble * 0.01
    logInfo("退款率：" + refundRateD)

    //计算出得分
    val oldScore: Double = 0.3 * Math.min(150, Math.max(0, 30 + 50 * Math.log(avgSaleOf12M / 100000D))) +
      0.35 * Math.min(150, Math.max(0, 170 - 280 * avgFlowOf3M)) +
      0.05 * Math.min(150, Math.max(0, 100)) +
      0.15 * Math.min(150, Math.max(0, -220 * avgTop3Of3M + 200)) +
      0.15 * Math.min(150, Math.max(0, -220 * Math.pow(avgDsrComp, 2) + 350 * avgDsrComp + 35))
    val newScore = Utils.retainDecimal(-0.004 * Math.pow(oldScore, 2) + 1.6 * oldScore, 0).toInt

    //授信额度
    val tempAmount = Math.min(avgSaleOf12M * newScore * (1 - refundRateD) / 100, 500000).toInt
    //以“万”为整。如34000算40000
    val creditAmount = if (tempAmount % 10000 == 0) tempAmount else (tempAmount / 10000 + 1) * 10000
    logInfo("授信额度：" + creditAmount)

    (newScore.toString, creditAmount.toString)
  }

  override private[service] def runServices(): Unit = {

    val workBook = new JDLPWorkBook
    val dsr = getDSR
    val refundRate = getRefundRate
    val saleInfoAYear = getSaleInfoAYear
    val avgSaleInfoAYear = avgSaleInfo(saleInfoAYear)
    val saleInfo3M = getSaleInfoOfLast3M
    val avgSaleInfo3M = avgSaleInfo(saleInfo3M)

    val (score, creditAmount) = getScoreAndCreditAmount(dsr, refundRate, avgSaleInfo3M, avgSaleInfoAYear)

    workBook.setContents(dsr, refundRate, toStringArray(saleInfo3M), avgSaleInfo3M, toStringArray(saleInfoAYear), avgSaleInfoAYear, score, creditAmount)

    val fileName = new UUIDGenerator().generateId() + ".xls"
    logInfo(s"文件名:$fileName")

    val localPath = s"${Constants.INPUT_FILE_PATH}/$fileName"
    logInfo(s"本地路径:$localPath")
    //将文件保存到本地
    workBook.saveToLocal(localPath)

    //上传文件到cos
    val fileUrl = uploadFile(fileName, localPath)

    sendFileMsgToMQ(fileUrl)
    sendScoreAndCreditToMQ(score, creditAmount)
  }
}
