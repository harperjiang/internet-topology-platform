package edu.clarkson.cs.scala.common

import java.io.FileInputStream

object Properties {

  val map = scala.collection.mutable.Map[String, java.util.Properties]();

  def load[T](file: String, key: String)(implicit m: Manifest[T]): T = {
    var stringval = map.getOrElse(file, {
      var prop = new java.util.Properties();
      var instream = new FileInputStream(file);
      prop.load(instream);
      instream.close;
      map += (file -> prop);
      prop
    }).getProperty(key);

    m.erasure.toString() match {
      case "int" => {
        return stringval.toInt.asInstanceOf[T];
      }
      case "long" => {
        return stringval.toLong.asInstanceOf[T];
      }
      case "bool" => {
        return stringval.toBoolean.asInstanceOf[T];
      }
      case _ => {
        return stringval.asInstanceOf[T];
      }
    }
  }
}