package com.asto.dmp.jdlp.service

import java.io.FileOutputStream
import com.asto.dmp.jdlp.base.Props
import org.apache.poi.hssf.usermodel._
import org.apache.poi.ss.usermodel._
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.XSSFWorkbook

class JDLPWorkBook {
  private val wb: Workbook = new HSSFWorkbook()
  //private val createHelper: CreationHelper = wb.getCreationHelper
  private val sheet: Sheet = wb.createSheet()
  //设置列宽
  Array(0, 1, 2, 3, 4, 5, 8).foreach(sheet.setColumnWidth(_, 10 * 256))
  Array(6, 7).foreach(sheet.setColumnWidth(_, 14 * 256))

  private val maxColumnIndex = 8

  private def defaultStyle(boldWeight: String = "normal") = {
    val defaultStyle: CellStyle = wb.createCellStyle()
    val dataFormat: DataFormat = wb.createDataFormat()
    defaultStyle.setDataFormat(dataFormat.getFormat("text"))

    val defaultFont: Font = wb.createFont()
    defaultFont.setFontHeightInPoints(10)
    defaultFont.setColor(IndexedColors.BLACK.getIndex)
    if (boldWeight == "normal") {
      defaultFont.setBoldweight(Font.BOLDWEIGHT_NORMAL)
    } else {
      defaultFont.setBoldweight(Font.BOLDWEIGHT_BOLD)
    }

    defaultFont.setFontName("宋体")
    defaultStyle.setFont(defaultFont)
    defaultStyle
  }

  private def firstRow = {
    //第一行
    val cellRange = new CellRangeAddress(0, 0, 0, maxColumnIndex)
    //在sheet里增加合并单元格
    sheet.addMergedRegion(cellRange)
    val row: Row = sheet.createRow(0)
    val cell: Cell = row.createCell(0)
    cell.setCellValue("元宝铺分析报告")
    val headFont: Font = wb.createFont()
    headFont.setBoldweight(Font.BOLDWEIGHT_BOLD)
    headFont.setFontHeightInPoints(22)
    headFont.setFontName("宋体")

    val headStyle = wb.createCellStyle()
    headStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER)
    headStyle.setAlignment(CellStyle.ALIGN_CENTER)
    headStyle.setFont(headFont)
    cell.setCellStyle(headStyle)
  }

  private def commonRow(rowNum: Int, contents: Array[String], fontWeight: Array[String], lastCellIndex: Int = maxColumnIndex) = {
    val row: Row = sheet.createRow(rowNum)
    (0 to lastCellIndex).foreach {
      i =>
        val cell: Cell = row.createCell(i)
        cell.setCellValue(contents(i))
        cell.setCellStyle(defaultStyle(fontWeight(i)))
    }
  }

  private def cellsMergeForRow(rowNum: Int, beginIndexOfCells: Array[Int], contents: Array[String], fontWeights: Array[String], lastCellIndex: Int = 8) = {
    val row: Row = sheet.createRow(rowNum)
    val cellsNum = beginIndexOfCells.length
    for (i <- beginIndexOfCells.indices) {
      val beginIndexOfCell = beginIndexOfCells(i)
      val endIndexOfCell = if (i == cellsNum - 1) lastCellIndex else beginIndexOfCells(i + 1) - 1

      if (beginIndexOfCell == endIndexOfCell) {
        //表示一个单元格不需要合并
        val cell: Cell = row.createCell(i)
        cell.setCellValue(contents(i))
        cell.setCellStyle(defaultStyle(fontWeights(i)))
      } else {
        //多个单元格合并
        val cellRange = new CellRangeAddress(rowNum, rowNum, beginIndexOfCell, endIndexOfCell)
        sheet.addMergedRegion(cellRange)
        val cell: Cell = row.createCell(beginIndexOfCell)
        cell.setCellValue(contents(i))
        cell.setCellStyle(defaultStyle(fontWeights(i)))
      }

    }
  }

  def save = {
    // Save
    var filename: String = Props.get("save_path")
    if (wb.isInstanceOf[XSSFWorkbook]) {
      filename = filename + "x"
    }

    val out: FileOutputStream = new FileOutputStream(filename)
    wb.write(out)
    out.close()
  }

  def setContents(dsr: Array[String], refundRate: String,
                  saleInfo3M: Array[Array[String]],
                  avgSaleInfo3M: Array[String],
                  saleInfoAYear: Array[Array[String]],
                  avgSaleInfoAYear: Array[String]
                   ) = {
    firstRow
    cellsMergeForRow(1, Array(0, 3, 5, 7), Array("借款人名称", "", "法人名称", ""), Array("bold", "normal", "bold", "normal"))
    cellsMergeForRow(2, Array(0, 3, 5, 7), Array("店铺名", "", "网店主营业务", ""), Array("bold", "normal", "bold", "normal"))
    cellsMergeForRow(3, Array(0, 3, 5, 7), Array("成立时间", "", "店铺等级", ""), Array("bold", "normal", "bold", "normal"))
    cellsMergeForRow(4, Array(0, 3, 5, 7), Array("DSR评分", "描述相符得分：", "服务态度得分：", "物流速度得分："), Array("bold", "bold", "bold", "bold"))

    cellsMergeForRow(5, Array(0, 3, 5, 7), Array("店铺DSR评分", dsr(0), dsr(2), dsr(4)), Array("bold", "normal", "normal", "normal"))
    cellsMergeForRow(6, Array(0, 3, 5, 7), Array("与同行业对比(高于)", dsr(1) + "%", dsr(3) + "%", dsr(5) + "%"), Array("bold", "normal", "normal", "normal"))
    cellsMergeForRow(7, Array(0, 3), Array("退款率", refundRate + "%"), Array("bold", "normal"))

    cellsMergeForRow(8, Array(0), Array("最近3个月销售分析"), Array("bold"))
    commonRow(9, Array("时间", "销售额", "浏览量", "访客数", "转化率", "客单价", "付费流量占比", "推广费用ROI", "商品结构"), Array("bold", "bold", "bold", "bold", "bold", "bold", "bold", "bold", "bold"))

    saleInfo3M.indices.foreach { i =>
      commonRow(10 + i, saleInfo3M(i), Array("bold", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"))
    }
    commonRow(13, avgSaleInfo3M, Array("bold", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"))
    commonRow(14, Array("各项得分", "", "", "", "", "", "", "", ""), Array("bold", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"))

    val creditAmount = avgSaleInfo3M(1) // 暂时取近三个月均值
    cellsMergeForRow(15, Array(0, 1, 5, 7), Array("总得分", "", "参考授信额度", creditAmount ), Array("bold", "normal", "bold", "normal"))
    cellsMergeForRow(16, Array(0), Array("最近12个月销售分析"), Array("bold"))
    commonRow(17, Array("时间", "销售额", "浏览量", "访客数", "转化率", "客单价", "付费流量占比", "推广费用ROI", "商品结构"), Array("bold", "bold", "bold", "bold", "bold", "bold", "bold", "bold", "bold"))

    saleInfoAYear.indices.foreach { i =>
      commonRow(18 + i, saleInfoAYear(i), Array("bold", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"))
    }

    commonRow(30, avgSaleInfoAYear, Array("bold", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"))
    commonRow(31, Array("各项评分", "", "", "", "", "", "", "", ""), Array("bold", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"))

  }
}


