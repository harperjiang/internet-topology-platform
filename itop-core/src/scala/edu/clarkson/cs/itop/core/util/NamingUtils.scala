package edu.clarkson.cs.itop.core.util

import java.util.UUID
import java.util.Date

object NamingUtils {

  def taskId: String = {
    "%s_%s".format(new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new Date()),
      UUID.randomUUID().toString)
  }
}