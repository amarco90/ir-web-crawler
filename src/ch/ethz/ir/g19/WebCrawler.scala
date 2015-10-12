package ch.ethz.ir.g19

import scala.collection.mutable.Set
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.io.BufferedSource
import java.io.FileNotFoundException
import scala.util.Random

object WebCrawler {
  // The Tuple2 -> (URL to parse, parent URL)
  val toParse = new Queue[String]
  val uniqueURLs = Set[String]()
  var notFoundResources = 0
  val uniqueEnglishPages = Set[String]()
  var uniqEngPages = 0;
//  var pageShingles = Set[String]()
  var pageText = ""
  var verbose = 0
  val n = 5;
  val gramLength = 3
  val pageHashes = new HashMap[String, String]
  var langDet : LanguageDetector = null
  var studentOccurrences = 0
  var permutedTables = HashMap[List[Int], List[(String, String)]]()
  var cc = 0

  def main(args: Array[String]) {
    if (args contains "-v")
      verbose = 1
    if (args contains "-vv")
      verbose = 2

    langDet = new LanguageDetector(gramLength, verbose)

    //definePermutations(5);
    
    val initParent =
        "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"
    val initResource = "en.html"
    val initURL = formatURL(initResource, initParent)
    toParse.enqueue(initURL)
    uniqueURLs.add(initURL)

    while (!toParse.isEmpty) {
      val url = toParse.dequeue()
      if (verbose >= 1)
        println("crawling: " + url)
      parseURL(url)
    }
    
    val dupli = searchDuplicates()
    println("-----------OUTPUT------------")
    println("Distinct URLs : " + (uniqueURLs.size - notFoundResources))
    println("Exact duplicates : " + dupli._2)
    println("Near duplicates : " + dupli._1)
    println("Unique English pages found : " + uniqEngPages)
    println("Term frequency of \"student\" : " + studentOccurrences)
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
        if (verbose >= 2)
          System.err.println("Resource not found: " + url)
        notFoundResources += 1
        return
      }
    }

    cc+=1
    
    println("---------------------------------" + cc + "---------------------------------")
    
    for (l <- sourceCode.getLines()) {
      // find new URLS
      urlRegex.findAllIn(l).matchData foreach {
        m => {
          val foundURL = anchorsAndParams.replaceAllIn(m.group(2), "")
          val parentURL = url.substring(0, url.lastIndexOf('/') + 1)
          val absoluteFoundURL = formatURL((foundURL, parentURL))
          if (foundURL != "" && foundURL.endsWith(".html") && !foundURL.contains("login")
              && !uniqueURLs.contains(absoluteFoundURL)) {
            toParse.enqueue(absoluteFoundURL)
            uniqueURLs.add(absoluteFoundURL)
            //if (verbose >= 2)
            //  println(" new URL found: " + absoluteFoundURL +
            //          " (" + foundURL + ")")
          } else if (!foundURL.endsWith(".html")) {
            if (verbose >= 2)
              println(" skipping not html resource " + foundURL)
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
      if(langDet.isEnglish(pageText)) /*uniqueEnglishPages.add(url)*/ uniqEngPages = uniqEngPages +1

       val tokens = pageText.split("[ .,;:?!\t\n\r\f]+").toList
       val shingles = tokens.sliding(n).toSet
       val hashes = shingles.map(_.hashCode).map { h => binary(h) }.toList
       
       val fingerprint = getFingerprint(hashes)
       //storePermutation(url, fingerprint)
       
       pageHashes.put(url, fingerprint)
       
    val currentStudent = "(?i)\\sstudent\\s".r.findAllIn(pageText).length
    studentOccurrences += currentStudent

    pageText = "";
  }

  def binary(value: Int) : String =
    String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0')

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
    if(n < 0) return 0
    else return 1
  }
  
  def getFingerprint(shingleSet: List[String]): String = {
      val listOfbitList = shingleSet.map { x => x.sliding(1).toList }

      val bigG = for {pos <- 0 until listOfbitList.apply(0).length} 
      yield listOfbitList.map { x => (x.apply(pos)).toInt*2-1 }.reduce(_+_)
        
      val smallG = bigG.map { x => sign(x)}.mkString
        
      return smallG
      
  }
  
  // Permutations: HashMap(pi_k -> List(URL, Fingerprint))
  
  def definePermutations(n: Int) /* : HashMap[List[Int], List[(String, String)]] = */{
    var iter = n
    
    while(iter != 0){
      val randomP = sample(0 to 30 toList, 20)
      if(!permutedTables.contains(randomP)){
        permutedTables.put(randomP, List())
        iter = iter - 1;  
      }
    }
  }
  
  def storePermutation(url: String, fingerprint: String/*, pTables: HashMap[List[Int], List[(String, String)]]*/)/*: HashMap[List[Int], List[(String, String)]] = */{
    
    val intFP = Integer.parseInt(fingerprint,2)
    
    val emptyMask = "0"
    
    for(pi_k <- permutedTables.keySet){
      var mask = emptyMask;
      mask = pi_k.foldLeft(mask)((s, i) => s.updated(i, '1'))
      val maskB = Integer.parseInt(mask,2)
      
      val permutedFP = (intFP & maskB).toBinaryString
      
     // permutedTables(pi_k) :+ (url, permutedFP)
      
      permutedTables.update(pi_k, permutedTables(pi_k):::List((url, permutedFP)))
    }
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
  
  def searchDuplicates(): (Int, Int) = {
    
    var ndCount = 0;
    var edCount = 0;
    
    print("page count: " + pageHashes.keySet.size)
    
    var t = Set[(String, String)]()
    
    val keyList = pageHashes.keySet.toList
    var countIter = 0
    
    for(i <- 0 until pageHashes.keySet.size;
        j <- i+1 until pageHashes.keySet.size){
        val key1 = keyList.apply(i)
        val key2 = keyList.apply(j)
      
        countIter +=1
        
        
        val hDist = hammingDistance(pageHashes(key1), pageHashes(key2))
      
       if(hDist == 0){
           edCount = edCount +1
       }else if(hDist > 0 && hDist < 4)
         ndCount = ndCount +1
    }

           return (ndCount, edCount)
  }
  
  def hammingDistance(s1: String, s2: String): Int = s1.zip(s2).count(c => c._1 != c._2)
  
}

