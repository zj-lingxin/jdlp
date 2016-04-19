package com.asto.dmp.jdlp.service.impl

import com.asto.dmp.jdlp.base.{Props, CsvDF}
import com.asto.dmp.jdlp.service.{JDLPWorkBook, Service}
import com.asto.dmp.jdlp.util.Utils

class MainService extends Service {
  val dynamic = CsvDF.load(Props.get("dynamic_path"))
  val detail = CsvDF.load(Props.get("detail_path"))
  val refund = CsvDF.load(Props.get("refund_path"))
  val summary = CsvDF.load(Props.get("summary_path"))
  val traffic = CsvDF.load(Props.get("traffic_path"))

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
      .map(a => ((a(0).toString), (a(1).toString.toDouble, a(2).toString.toInt, a(3).toString.toInt, a(4).toString.toDouble, a(5).toString.toDouble)))
    val flowRate = traffic.select("当前月", "流量占比").map(a => (a(0).toString, a(1).toString.toDouble))
    val top3Amount = detail.select("当前月", "下单金额").map(a => (a(0).toString, a(1).toString.toDouble)).reduceByKey(_ + _)
    info.cogroup(flowRate, top3Amount).map(t => (t._1.toString, t._2._1.head, t._2._2.head, t._2._3.head))
      .map(t => (t._1, t._2._1, t._2._2, t._2._3, Utils.retainDecimal(t._2._4 * 100), Utils.retainDecimal(t._2._5), Utils.retainDecimal(t._3 * 100), "", Utils.retainDecimal(t._4 / t._2._1 * 100)))
      .sortBy(_._1, false).persist()
  }

  def getSaleInfoOfLast3M = {
    getSaleInfo.top(3).sortBy(_._1)
  }

  def avgSaleInfo(saleInfo: Array[(String, Double, Int, Int, Double, Double, Double, String, Double)], monthNum: Int): Array[String] = {
    val num = monthNum.toDouble
    val sum = saleInfo.reduce((a, b) => ("均值", a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9))
    Array(
      "均值",
      Utils.retainDecimal(sum._2 / num).toString,
      Utils.retainDecimal(sum._3 / num).toString,
      Utils.retainDecimal(sum._4 / num).toString,
      Utils.retainDecimal(sum._5 / num).toString + "%",
      Utils.retainDecimal(sum._6 / num).toString,
      Utils.retainDecimal(sum._7 / num).toString + "%",
      "",
      Utils.retainDecimal(sum._9 / num).toString + "%"
    )
  }

  def getSaleInfoAYear = {
    getSaleInfo.top(12).sortBy(_._1)
  }

  def toStringArray(array: Array[(String, Double, Int, Int, Double, Double, Double, String, Double)]):Array[Array[String]] = {
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
    val avgSaleInfoAYear = avgSaleInfo(saleInfoAYear, 12)
    val saleInfo3M = getSaleInfoOfLast3M
    val avgSaleInfo3M = avgSaleInfo(saleInfo3M, 3)

    workBook.setContents(dsr, refundRate, toStringArray(saleInfo3M), avgSaleInfo3M, toStringArray(saleInfoAYear), avgSaleInfoAYear)
    workBook.save

  }
}

