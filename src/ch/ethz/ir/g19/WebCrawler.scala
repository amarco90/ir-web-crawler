package ch.ethz.ir.g19

import java.io._
import collection.mutable.Map
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import java.net.URL

object WebCrawler {
  // The Tuple2 -> (URL to parse, parent URL)
  val toParse = new Stack[Tuple2[String, String]]
  val uniqueURLs = Set[String]()
//  var pageShingles = Set[String]()
  var pageText = ""
  var verbose = false
  val n = 5;
  val nGram = 3
  val pageHashes = new HashMap[String, List[String]]

  var germanModel : Map[String, Double] = null
  var englishModel : Map[String, Double] = null

  def main(args: Array[String]) {
    if (args contains "-v")
      verbose = true

    val initPage =
        "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html"
    toParse.push((initPage, ""))

    englishModel = loadModel("models/english")
    germanModel = loadModel("models/german")
    //while (!toParse.isEmpty) {
      if (verbose)
        println("crawling: " + initPage)
      readURL(toParse.pop())
    //}
  }

  def loadModel(modelPath : String) : Map[String, Double] = {
    val ois = new ObjectInputStream(new FileInputStream(modelPath))
    val model = ois.readObject()
    ois.close()
    
    return model.asInstanceOf[Map[String, Double]]
  }

  def isEnglish(grams : IndexedSeq[String]) : Boolean = {
    val probEnglish = 
      grams.map(t => Math.log(englishModel.getOrElse(t, 0.0)
          .asInstanceOf[Double] + 1)).reduce(_ + _)
    val probGerman = 
      grams.map(t => Math.log(germanModel.getOrElse(t, 0.0)
          .asInstanceOf[Double] + 1)).reduce(_ + _)
    if (verbose) {
      println("P(english)=" + probEnglish)
      println("P(german)=" + probGerman)
    }
    return probEnglish > probGerman
  }

  def getGrams(text : String) : IndexedSeq[String] = {
    val normalizedText = pageText.toLowerCase.replaceAll("[\\d()\"]+", "")
    val grams = for {
      start <- 0 to normalizedText.length
      if start + nGram <= normalizedText.length
    } yield normalizedText.substring(start, start + nGram)
    return grams
  }

  def readURL(tupleURL: Tuple2[String, String]) {
    val urlRegex = "(<a.*href=\")((?!http)[^\\s]+)(\")".r
    val textRegex = "(>)([^<>]+[a-zA-Z0-9]+)".r
    val sourceCode = io.Source.fromURL(tupleURL._1);

    for (l <- sourceCode.getLines()) {
      // find new URLS
      urlRegex.findAllIn(l).matchData foreach {
        m => {
          val parent = tupleURL._2
          if (parent == "") tupleURL._1 // in case it's the first URL
          if (m.group(2).charAt(0) != '#')
            // TODO check not in unique urls
            toParse.push((m.group(2), ""))
          if (verbose) println(" new URL found: " + m.group(2))
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
    

    val grams = getGrams(pageText)
    isEnglish(grams)

       val tokens = pageText.split("[ .,;:?!\t\n\r\f]+").toList
       val shingles = tokens.sliding(n).toSet
       val hashes = shingles.map(_.hashCode).map { h => binary(h) }.toList
       
       val hashPermutedList = permutations(hashes)
       
       pageHashes.put(tupleURL._2, hashPermutedList)
    
       
       
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
  
  def permutations(shingleSet: List[String]): List[String] = {
      val listOfbitList = shingleSet.map { x => x.sliding(1).toList }

      val test = List(List("1","1","1","1"), List("0","1","0","1"))
      val bigG = for {pos <- 0 until listOfbitList.apply(0).length}
        yield listOfbitList.map { x => (x.apply(pos)).toInt*2-1 }.reduce(_+_)
        
      val smallG = bigG.map { x => sign(x)}
        
      val permutations = Stream.continually(smallG.reverse).flatten.sliding(smallG.size).map(_.reverse)
      
      val permutationList = for { l <- 1 until 32 }
        yield permutations.take(1).flatten.mkString   
        
        return permutationList.toList
      
  }

  def streamTokens(path : String, n : Int) : Iterator[String] = {
    val tokens = for {
      line <- io.Source.fromFile(path).getLines.map(l => l.toLowerCase)
          .map(l => l.replaceAll("[\\d()\"]+", "")).take(200)
      start <- 0 to line.length
      if start + n <= line.length
    } yield line.substring(start, start + n)
    return tokens
  }
  
  def searchDuplicates() = {
    

    
  }
}

