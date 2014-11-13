package edu.clarkson.cs.itop.core.tool

import com.google.gson.GsonBuilder
import java.util.Random
import java.util.regex.Pattern

object ScalaSnippets extends App {

  var pattern = Pattern.compile("(\\d+)\\s+(\\d+)");
  var matcher = pattern.matcher("17 20");
  if (matcher.matches()) {
    System.out.println(matcher.group(1));
    System.out.println(matcher.group(2));
  }
}