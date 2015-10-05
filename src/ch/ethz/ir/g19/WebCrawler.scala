package ch.ethz.ir.g19

import scala.collection.mutable.Stack
import java.net.URL

object WebCrawler {
  // The Tuple2 -> (URL to parse, parent URL)
  val toParse = new Stack[Tuple2[String, String]]
  val uniqueURLs = Set[String]()
  var verbose = false

  def main(args: Array[String]) {
    if (args contains "-v")
      verbose = true

    val initPage =
        "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html"

    if (verbose)
      println("Let's crawl! Origin URL: " + initPage)
    readURL((initPage, ""))
    //while (!toParse.isEmpty) {
    //  readURL(toParse.pop())
    //}
  }

  def readURL(tupleURL: Tuple2[String, String]) {
    val urlRegex = "(<a.*href=\")((?!http)[^\\s]+)(\")".r
    val sourceCode = io.Source.fromURL(tupleURL._1);

    for (l <- sourceCode.getLines()) {
      urlRegex.findAllIn(l).matchData foreach {
        m => {
          val parent = tupleURL._2
          if (parent == "") tupleURL._1 // in case it's the first URL
          // TODO check not in unique urls
          toParse.push((m.group(2), ""))
          if (verbose) println("new URL found: " + m.group(2))
        }
      }
    }
  }
  

  // first element = url
  // second element = parent
  def formatURL(element: Tuple2[String, String]): String = {
      val parts = element._1.split('/').foldLeft(element._2.split('/')) { 
      case (cur, dir) =>
        if (dir == ".") cur // stay in current directory
        else if (dir == "..") cur.dropRight(1) // go up in the tree
        else cur :+ dir // cd into directory
    }
    return parts.mkString("/") // build the string back
  }

}

