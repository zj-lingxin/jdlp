package com.asto.dmp.jdlp.mq

import com.asto.dmp.jdlp.base.Constants
import com.asto.dmp.jdlp.util.DateUtils

import scala.util.parsing.json.{JSONArray, JSONObject}

object MsgWrapper {
  def getJson(quotaItemName: String, msgList: List[Msg], propertyUuid: String): String = {
    new JSONObject(Map(
      "quotaItemName" -> quotaItemName,
      "propertyUuid" -> propertyUuid,
      "quotaItemList" -> JSONArray(for (msg <- msgList) yield matchMsgType(msg))
    )).toString()
  }

  def matchMsgType(msg: Msg) = {
    msg match {
      case _msg: MsgWithName =>
        toMsgWithNameJsonObj(_msg)
      case _ =>
        toJsonObj(msg)
    }
  }

  def toJsonObj(msg: Msg): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> msg.indexFlag, "quotaCode" -> msg.quotaCode, "targetTime" -> msg.targetTime, "quotaValue" -> msg.quotaValue))
  }

  def toMsgWithNameJsonObj(msg: MsgWithName): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> msg.indexFlag, "quotaName" -> msg.quotaName, "quotaCode" -> msg.quotaCode, "targetTime" -> msg.targetTime, "quotaValue" -> msg.quotaValue))
  }
}

object Msg {
  def apply(quotaCode: String, quotaValue: Any, indexFlag: String = "2", targetTime: String = DateUtils.getStrDate("yyyyMM")) = {
    new Msg(quotaCode, quotaValue, indexFlag, targetTime)
  }
  def strMsgsOfAStore(quotaItemName: String, propertyUuid: String, msgs: List[Msg]): String = {
    val buffer = new StringBuffer()
    for (msg <- msgs) {
      buffer.append(s"$quotaItemName${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"$propertyUuid${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.quotaCode}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.indexFlag}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.targetTime}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.quotaValue}\n")
    }
    buffer.toString
  }
}

class Msg(val quotaCode: String, val quotaValue: Any, val indexFlag: String = "2", val targetTime: String = DateUtils.getStrDate("yyyyMM"))

object MsgWithName {
  def apply(quotaCode: String, quotaName: String, quotaValue: Any, indexFlag: String = "2", targetTime: String = DateUtils.getStrDate("yyyyMM")) = {
    new MsgWithName(quotaCode, quotaName, quotaValue, indexFlag, targetTime)
  }
  def strMsgsOfAStore(quotaItemName: String, propertyUuid: String, msgs: List[MsgWithName]) = {
    val buffer = new StringBuffer()
    for (msg <- msgs) {
      buffer.append(s"$propertyUuid${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"$quotaItemName${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.quotaCode}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.quotaName}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.indexFlag}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.targetTime}${Constants.OUTPUT_SEPARATOR }")
      buffer.append(s"${msg.quotaValue}\n")
    }
    buffer.toString
  }
}

class MsgWithName(override val quotaCode: String, val quotaName: String, override val quotaValue: Any, override val indexFlag: String, override val targetTime: String) extends Msg(quotaCode, quotaValue, indexFlag, targetTime)