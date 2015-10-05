package ch.ethz.ir.g19

import scala.collection.mutable.Stack
import java.net.URL

object WebCrawler {
  // The Tuple2 -> (URL to parse, parent URL)
  val toParse = new Stack[Tuple2[String, String]]

  def main(args: Array[String]) {
    val initPage = "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html"

    readURL((initPage, ""))
    while (!toParse.isEmpty) {
      readURL(toParse.pop())
    }
  }

  def readURL(tupleURL: Tuple2[String, String]) {
    val urlRegex = "(<a.*href=\")((?!http)[^\\s]+)(\")".r
    val sourceCode = io.Source.fromURL(tupleURL._1);

    for (l <- sourceCode.getLines()) {
      urlRegex.findAllIn(l).matchData foreach {
        m => toParse.push((m.group(2), ""))
      }
    }
  }

}

