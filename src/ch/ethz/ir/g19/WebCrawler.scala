package ch.ethz.ir.g19

import scala.collection.mutable.Set
import scala.collection.mutable.MutableList
import scala.collection.immutable.{Set => ImSet}
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.io.BufferedSource
import java.io.FileNotFoundException
import scala.util.Random

object WebCrawler {
  val toParse = new Queue[String]
  val uniqueURLs = Set[String]()
  var notFoundResources = 0
  val uniqueEnglishPages = Set[String]()
  val fullTextHash = HashMap[Int, MutableList[String]]()
  val jaccardHash = HashMap[String, ImSet[Int]]()
  var uniqEngPages = 0
//  var pageShingles = Set[String]()
  var pageText = ""
  var verbose = 0
  var cntloginPerso = 0;
  val n = 5
  val gramLength = 3
  val pageHashes = new HashMap[String, List[Int]]
  var langDet : LanguageDetector = null
  var studentOccurrences = 0
 // var permutedTables = HashMap[List[Int], List[(String, String)]]()
  var permutedTables = HashMap[List[Int], HashMap[Int, Set[String]]]()
  var cc = 0
  var countED = 0
  
  var fuckthis = 0;

  def main(args: Array[String]) {
    if (args contains "-v")
      verbose = 1
    if (args contains "-vv")
      verbose = 2

    langDet = new LanguageDetector(gramLength, verbose)

    definePermutations(5);
    
    val initParent =
        "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/"
    val initResource = "en.html"
    val initURL = formatURL(initParent, initResource)
    toParse.enqueue(initURL)
    uniqueURLs.add(initURL)
    
    val start = System.currentTimeMillis()

    while (!toParse.isEmpty/* && fuckthis < 1500*/) {
      val url = toParse.dequeue()
      if (verbose >= 1)
        println("crawling: " + url)
      parseURL(url)
    }
    
    val time = start - System.currentTimeMillis()
    
    println("Pages per seconds : " + (pageHashes.keySet.size / (time / 1000)).toDouble)
    
    
    
    val dupli = searchDuplicates2()
    println("-----------OUTPUT------------")
    println("Distinct URLs: " + (uniqueURLs.size - notFoundResources) +" login/personen : " + cntloginPerso)
    println("Exact duplicates: " + countED)
    println("Near duplicates : " + dupli)
    println("Unique English pages found: " + uniqEngPages)
    println("Term frequency of \"student\": " + studentOccurrences)
  }

  def parseURL(url: String) {
    val urlRegex = "(href=\")([^\"]*)".r
    val textRegex = "(>)([^<>]+[a-zA-Z0-9]+)".r
    val anchorsAndParams =  "(#.*)|(\\?.*)".r
    val loginRegex = "(login[a-z0-9]{4})|(personendetail[a-z0-9]{4})".r
    var sourceCode : BufferedSource = null
    try {
      sourceCode = io.Source.fromURL(url)
    } catch {
      case fnfe: FileNotFoundException => {
        if (verbose >= 2)
          System.err.println(" Resource not found: " + url)
        notFoundResources += 1
        return
      }
    }

    cc+=1
    fuckthis +=1
    println("---------------------------------" + cc + "---------------------------------")
    
    for (l <- sourceCode.getLines()) {
      // find new URLS
      urlRegex.findAllIn(l).matchData foreach {
        m => {
          val foundURL = anchorsAndParams.replaceAllIn(m.group(2), "")
          val parentURL = url.substring(0, url.lastIndexOf('/') + 1)
          val absoluteFoundURL = formatURL(parentURL, foundURL)
          if (foundURL != ""  && (!foundURL.startsWith("http") || foundURL.startsWith("http://idvm-infk-hofmann03.inf.ethz.ch/"))
              && foundURL.endsWith(".html")
              && !uniqueURLs.contains(absoluteFoundURL)) {
         //   if(loginRegex.findFirstIn(foundURL) == None){
            toParse.enqueue(absoluteFoundURL)
            uniqueURLs.add(absoluteFoundURL)
       /*     }
            else{
              cntloginPerso+=1
            }*/
            
            if (verbose >= 2)
              println(" new URL found: " + absoluteFoundURL +
                      " (" + foundURL + ")")
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
    pageText = pageText.replaceAll("&nbsp;?", " ").replaceAll("&[Aa]uml;", "ä")
        .replaceAll("&[Oo]uml;", "ö").replaceAll("&[Uu]uml;", "ü")
        
        val currentStudent = "(?i)\\sstudent\\s".r.findAllIn(pageText).length
        studentOccurrences += currentStudent
        
        val textHash = pageText.hashCode()
        if(fullTextHash.contains(textHash)){
          val collisions = fullTextHash.apply(textHash)
          if(collisions.contains(pageText)){
            countED += 1
          }else{
            collisions+=pageText
          }
        }else{ 
          fullTextHash.put(textHash, MutableList(pageText))
          if (pageText != "")
            if(langDet.isEnglish(pageText)) uniqEngPages = uniqEngPages +1
      
             val tokens = pageText.split("[ .,;:?!\t\n\r\f]+").toList.map { x => x.toLowerCase() }
             val shingles = tokens.sliding(n).toSet
             
             val setHashShingles = shingles.map {_.hashCode()}.toSet
             
             jaccardHash.put(url, setHashShingles)
             
             val hashes = setHashShingles.map { h => binary(h) }.toList
             
             val fingerprint = simHash(hashes)
             storePermutation(url, fingerprint)
             
             pageHashes.put(url, fingerprint)
        }
    
    

    pageText = ""
  }

  def binary(value: Int) : String =
    String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0')

  def formatURL(parentURL: String, foundURL: String): String = {
    val parts = foundURL.split('/').foldLeft(parentURL.split('/')) {
      case (cur, dir) =>
        if (dir == ".") cur // stay in current directory
        else if (dir == "..") cur.dropRight(1) // go up in the tree
        else cur :+ dir // cd into directory
    }
    return parts.mkString("/") // build the string back
  }
    
 def sign(n:Int): Int = {
    if(n < 0) return 0
    else return 1
  }
  
  def simHash(shingleSet: List[String]): List[Int] = {
      val listOfbitList = shingleSet.map { x => x.sliding(1).toList.map(_.toString.toInt) }
      
      val bigG = for {pos <- 0 until listOfbitList.apply(0).length} 
      yield listOfbitList.map { x => (x.apply(pos))*2-1 }.reduce(_+_)
        
      val smallG = bigG.map { x => sign(x)}.toList
        
      return smallG
      
  }
  
  def definePermutations(n: Int) {
    var iter = n
    
    while(iter != 0){
      val randomP = sample(0 to 30 toList, 10)
      if(!permutedTables.contains(randomP)){
        permutedTables.put(randomP, HashMap[Int, Set[String]]())
        iter = iter - 1;  
      }
    }
  }
  
  def storePermutation(url: String, fingerprint: List[Int]) {
    
    val emptyMask = "00000000000000000000000000000000"
    
    val fpInt = convertBinaryListToInt(fingerprint)
    
    for(pi_k <- permutedTables.keySet){
      val maskL = pi_k.foldLeft(emptyMask)((s, i) => s.updated(i, '1')).toList.map (_.toString.toInt)

      val maskInt = convertBinaryListToInt(maskL)
      
      val permutedFPInt =  fpInt & maskInt
      /*println("####################################### permutation : " + pi_k.sortWith(_ < _))
      println("####################################### permutation : " + pi_k)
      println("####################################### original FP : " + binary(fpInt))
      println("#######################################        mask : " + binary(maskInt))
      println("####################################### permuted FP : " + binary(permutedFPInt))
      println("####################################### original FP : " + fpInt)
      println("#######################################        mask : " + maskInt)
      println("####################################### permuted FP : " + permutedFPInt)*/
      
      val FPHashtable = permutedTables(pi_k)
      
      val urlList = FPHashtable.getOrElse(permutedFPInt, Set())
      
     // urlList.add(url)
      
      FPHashtable.update(permutedFPInt, urlList += url)
      
      //FPHashtable.update(permutedFPInt, url :: urlList)
      
       permutedTables.update(pi_k, FPHashtable)

     /* println("#######################################         urlList : " + urlList)
      println("#######################################     FPHashtable : " + FPHashtable)
      println("####################################### permutedTables2 : " + permutedTables2)*/
      
 
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
  
  
  def convertBinaryListToInt(binList: List[Int]): Int = 
    (0 to 31).foldLeft(0){(a,index) => a + (1<<index)*binList.reverse(index) }
  
  def searchDuplicates2(): Int = {
    
    var cntND = 0
    var cntED = 0
    
    
    val keyList = pageHashes.keySet.toList
    var countIter = 0
    val emptyMask = "00000000000000000000000000000000"
    
    
    for(i <- 0 until pageHashes.keySet.size){
      var candidates = Set[String]()
      val urlQ = keyList.apply(i)
      val fp = pageHashes.getOrElse(urlQ, Nil)
      
        
        val fpInt = convertBinaryListToInt(fp)
      
      for(pi_k <- permutedTables.keySet){
       
        val maskL = pi_k.foldLeft(emptyMask)((s, i) => s.updated(i, '1')).toList.map (_.toString.toInt)
        val maskInt = convertBinaryListToInt(maskL)
    
        val query = fpInt & maskInt
    
        candidates = candidates ++ permutedTables(pi_k).getOrElse(query, Set())
        
        countIter +=1

       }
      
      
      println("Number of candidates for " + urlQ+" ("+fpInt+") : " + candidates.toSet.size)
      
      val fpCandidates = for{ url <- candidates} yield ((pageHashes.getOrElse(url, List()), url))
      
      val counts = compareHamming((fp, urlQ), fpCandidates)
      cntND += counts
      
      
      
      
    }
    
    return cntND
    
  }

  
  def compareHamming(queryFP : (List[Int], String), candidates : Set[(List[Int], String)]): Int = {
    def hammingDistance(s1: String, s2: String): Int = s1.zip(s2).count(c => c._1 != c._2)
    
    val cSet = candidates
    
    var cntND = 0;
    var cntED = 0;
    val qStr = queryFP._1.mkString
    val urlQ = queryFP._2
    
    for{candidate <- cSet}{
      val candidateStr = candidate._1.mkString
      val candidateUrl = candidate._2
      
      if(!qStr.equals(candidateStr) && !urlQ.equals(candidateUrl)){
        if(hammingDistance(qStr, candidateStr) <= 6){
          if(jaccardSimilarity(urlQ, candidateUrl) > 0.85)
          cntND += 1
        }
      }
      
    }
    
    return cntND
     
  }
  
  def jaccardSimilarity(url1: String, url2: String): Double = {
    
    val url1Set = jaccardHash.getOrElse(url1, ImSet())
    val url2Set = jaccardHash.getOrElse(url2, ImSet())
    
    val sim = (url1Set intersect url2Set).size.toDouble/(url1Set union url2Set).size
    
    return sim
    
  }
  
  
  
}

