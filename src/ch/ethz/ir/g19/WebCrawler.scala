package ch.ethz.ir.g19

import scala.collection.mutable.Stack
import java.net.URL

object WebCrawler {
  // The Tuple2 -> (URL to parse, parent URL)
  val toParse = new Stack[Tuple2[String, String]]
  val uniqueURLs = Set[String]()
//  var pageShingles = Set[String]()
  var pageText = ""
  var verbose = false
  val n = 5;

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
    val textRegex = "(>)([^<>\\n]+[a-zA-Z0-9]+)".r
    val sourceCode = io.Source.fromURL(tupleURL._1);

    for (l <- sourceCode.getLines()) {
      // find new URLS
      urlRegex.findAllIn(l).matchData foreach {
        m => {
          val parent = tupleURL._2
          if (parent == "") tupleURL._1 // in case it's the first URL
          // TODO check not in unique urls
          toParse.push((m.group(2), ""))
          if (verbose) println("new URL found: " + m.group(2))
        }
      }
      // Get textual content
      textRegex.findAllIn(l).matchData foreach {
        m => {
          val text = m.group(2)
          pageText += text + " "
        }
        
      }
    }
    
       val tokens = pageText.split("[ .,;:?!\t\n\r\f]+").toList
       val shingles = tokens.sliding(n).toSet
       val hashes = shingles.map(_.hashCode).map { h => binary(h) }.toList
    
       
       
       pageText = "";
       
  }
  
    def binary(value: Int) : String =
      String.format("%16s", Integer
          .toBinaryString(value))
          .replace(' ', '0')

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
    
  def sign(n:Integer): Integer = {
    if(n < 1) return 0
    else return 1
  }
  
  def permutations(shingleSet: List[String]) = {
      val listOfbitList = shingleSet.map { x => x.sliding(1).toList }

      val test = List(List("1","1","1","1"), List("0","1","0","1"))
      val bigG = for {pos <- 0 until listOfbitList.apply(0).length}
        yield listOfbitList.map { x => (x.apply(pos)).toInt*2-1 }.reduce(_+_)
        
      val smallG = bigG.map { x => sign(x)}
        
        //TODO :  n-bits permutations
      
      
  }

}

