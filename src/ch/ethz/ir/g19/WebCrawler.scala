package ch.ethz.ir.g19

import scala.collection.mutable.Set
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.io.BufferedSource
import java.io.FileNotFoundException
import scala.util.Random

object WebCrawler {
  // The Tuple2 -> (URL to parse, parent URL)
  val toParse = new Stack[String]
  val uniqueURLs = Set[String]()
//  var pageShingles = Set[String]()
  var pageText = ""
  var verbose = 0
  val n = 5;
  val gramLength = 3
  val pageHashes = new HashMap[String, List[String]]
  var langDet : LanguageDetector = null
  var studentOccurrences = 0

  def main(args: Array[String]) {
    if (args contains "-v")
      verbose = 1
    if (args contains "-vv")
      verbose = 2

    langDet = new LanguageDetector(gramLength, verbose)

    val initParent =
        "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"
    val initResource = "en.html"
    val initURL = formatURL(initResource, initParent)
    toParse.push(initURL)
    uniqueURLs.add(initURL)

    while (!toParse.isEmpty) {
      val url = toParse.pop()
      if (verbose >= 1)
        println("crawling: " + url)
      parseURL(url)
    }
  }

  def parseURL(url: String) {
    val urlRegex = "(<a.*href=\")((?!http)[^\\s]+)(\")".r
    val textRegex = "(>)([^<>]+[a-zA-Z0-9]+)".r
    val anchorsAndParams =  "(#.*)|(\\?.*)".r
    var sourceCode : BufferedSource = null
    try {
      sourceCode = io.Source.fromURL(url);
    } catch {
      case fnfe: FileNotFoundException => {
        System.err.println("Resource not found: " + url)
        return
      }
    }

    for (l <- sourceCode.getLines()) {
      // find new URLS
      urlRegex.findAllIn(l).matchData foreach {
        m => {
          val foundURL = anchorsAndParams.replaceAllIn(m.group(2), "")
          val parentURL = url.substring(0, url.lastIndexOf('/') + 1)
          val absoluteFoundURL = formatURL((foundURL, parentURL))
          if (foundURL != "" && foundURL.endsWith(".html")
              && !foundURL.contains("login") // login sites are never found
              && !uniqueURLs.contains(absoluteFoundURL)) {
            toParse.push(absoluteFoundURL)
            uniqueURLs.add(absoluteFoundURL)
            if (verbose >= 2)
              println(" new URL found: " + absoluteFoundURL +
                      " (" + foundURL + ")")
          } else {
            if (verbose >= 2)
              println(" skipping not html resource " + absoluteFoundURL)
          }
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

    if (pageText != "")
      langDet.isEnglish(pageText)

       /*val tokens = pageText.split("[ .,;:?!\t\n\r\f]+").toList
       val shingles = tokens.sliding(n).toSet
       val hashes = shingles.map(_.hashCode).map { h => binary(h) }.toList
       
       val hashPermutedList = permutations(hashes)
       
       pageHashes.put(url, hashPermutedList)*/
    val currentStudent = "student".r.findAllIn(pageText).length
    studentOccurrences += currentStudent

    pageText = "";
  }

  def binary(value: Int) : String =
    String.format("%31s", Integer
        .toBinaryString(value)).replace(' ', '0')

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
  
  def getFingerprint(shingleSet: List[String]): String = {
      val listOfbitList = shingleSet.map { x => x.sliding(1).toList }

   //   val test = List(List("1","1","1","1"), List("0","1","0","1"))
      val bigG = for {pos <- 0 until listOfbitList.apply(0).length} yield listOfbitList.map { x => (x.apply(pos)).toInt*2-1 }.reduce(_+_)
        
      val smallG = bigG.map { x => sign(x)}.mkString
        
      return smallG
      
  }
  
  // Permutations: HashMap(pi_k -> List(URL, Fingerprint))
  
  def definePermutations(n: Int) : HashMap[List[Int], List[(String, String)]] = {
    val permutedTables = HashMap[List[Int], List[(String, String)]]()
    
    var iter = n
    
    while(iter != 0){
      val randomP = sample(0 to 30 toList, 20)
      if(!permutedTables.contains(randomP)){
        permutedTables.put(randomP, List())
        iter = iter - 1;  
      }
    }
    
   return permutedTables
    
  }
  
  
  
  def storePermutation(url: String, fingerprint: String, pTables: HashMap[List[Int], List[(String, String)]]): HashMap[List[Int], List[(String, String)]] = {
    
    val intFP = Integer.parseInt(fingerprint,2)
    val emptyMask = "0000000000000000000000000000000"
    
    

    for(pi_k <- pTables.keySet){
      var mask = emptyMask;
      mask = pi_k.foldLeft(mask)((s, i) => s.updated(i, '1'))
      val maskB = Integer.parseInt(mask,2)
      
      val permutedFP = (intFP & maskB).toBinaryString
      
      pTables(pi_k) :+ (url, permutedFP)
      
    }
    
    return pTables;
  
    
  }
  
  def sample[A](itms:List[A], sampleSize:Int) = {

        def collect(vect: Vector[A], sampleSize: Int, acc : List[A]) : List[A] = {
            if (sampleSize == 0) acc
            else {
                val index = Random.nextInt(vect.size)
                collect( vect.updated(index, vect(0)) tail, sampleSize - 1, vect(index) :: acc)
            }
        }

        collect(itms toVector, sampleSize, Nil)
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

