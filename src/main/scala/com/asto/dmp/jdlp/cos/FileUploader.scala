package com.asto.dmp.jdlp.cos

import com.asto.dmp.jdlp.base.Props
import com.qcloud.cosapi.api.CosCloud
import org.apache.spark.Logging
import org.json.JSONObject

class FileUploader extends Logging {

  val appId = Props.get("cos_app_id").toInt
  val secretId = Props.get("cos_secret_id")
  val secretKey = Props.get("cos_secret_key")
  val bucket = Props.get("cos_bucket")
  val dir = Props.get("cos_dir")
  val cos: CosCloud = new CosCloud(appId, secretId, secretKey)

  def deleteAndUpload(fileName: String, localPath: String) = {
    val remotePath = dir + fileName
    logInfo(s"remotePath:$remotePath")
    logInfo(s"localPath:$localPath")
    cos.deleteFile(bucket, remotePath)
    cos.uploadFile(bucket, remotePath, localPath)
  }

  /**
   * 上传成功返回下载地址，上传失败返回空字符串
   */
  def upload(fileName: String, localPath: String): String = {
    var result = ""
    result = deleteAndUpload(fileName: String, localPath: String)
    if (new JSONObject(result).getInt("code") != 0)
      ""
    else
      new JSONObject(result).getJSONObject("data").getString("resource_path")
  }

}
