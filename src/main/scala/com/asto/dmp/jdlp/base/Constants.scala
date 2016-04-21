package com.asto.dmp.jdlp.base

object Constants {
  val APP_NAME = "京东罗盘"
  var INPUT_FILE_PATH: String = _
  val OUTPUT_SEPARATOR = "\t"

  object ShopInfo {
    //京东帐号，也是jdid
    var JD_NAME: String = _
    //店铺名称
    var SHOP_NAME: String = _
    //借款人名称
    var USER_NAME: String = _
    //店铺等级
    var SHOP_LEVEL:String = _
    //法人名称
    var COMPANY_NAME:String = _
    //网店主营业务
    var MAJOR_BUSINESS:String = _
  }
}

