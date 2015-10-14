package ch.ethz.ir.g19

import java.io.FileNotFoundException
import java.net.MalformedURLException
import java.net.URL

import scala.collection.mutable.{ Set => MutSet }
import scala.collection.mutable.MutableList
import scala.collection.immutable.Set //{ Set => ImSet }
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.io.BufferedSource
import scala.util.Random

object WebCrawler {
  type Shingle = List[String]

  var verbose = 0 // verbosity level
  val DefaultURL =
    "http://idvm-infk-hofmann03.inf.ethz.ch/eth/www.ethz.ch/en.html"

  val toParse = new Queue[URL] // Frontier of URLs to parse
  val uniqueURLs = MutSet[URL]() // Unique URLs found
  var notFoundResources = 0 // URLs that give loading error

  val fingerprints = new HashMap[Int, MutableList[String]] // for exact dup

  var uniqEngPages = 0
  var countED = 0 // count Exact Duplicates

  val jaccardHash = HashMap[String, Set[Int]]()
  val Domain = "http://idvm-infk-hofmann03.inf.ethz.ch/"

  var cntloginPerso = 0;
  val q = 5 // q-grams length
  val gramLength = 3
  val pageHashesMinHash = MutableList[Tuple2[List[String], String]]()
  var exactDuplicates = 0
  var nearDuplicates = 0
  val nearDuplicateThreshold = 0.95
  val pageHashesSimHash = new HashMap[String, List[Int]]
  var studentOccurrences = 0
  val numRepetitions = 70
  val permCodes = generatePermutationCodes(32, numRepetitions)
  var permutedTables = HashMap[List[Int], HashMap[Int, MutSet[String]]]()
  var visitedURLs = 0

  var langDet: LanguageDetector = null
  var countED = 0 // count Exact Duplicates
  var countND = 0 // count Near Duplicates

  def main(args: Array[String]) {
    if (args contains "-v")
      verbose = 1
    if (args contains "-vv")
      verbose = 2

    var initURL: URL = null
    if (args.length > 0 && args(args.length - 1).startsWith("http"))
      initURL = new URL(args(args.length - 1))
    else
      initURL = new URL(DefaultURL)

    langDet = new LanguageDetector(gramLength, verbose)

    definePermutations(8, 10);

    toParse.enqueue(initURL)
    uniqueURLs.add(initURL)

    val start = System.currentTimeMillis()

    val startTime = System.currentTimeMillis()
    while (!toParse.isEmpty) {
      val url = toParse.dequeue()
      visitedURLs +=1
      if (verbose >= 1)
        println("[" + visitedURLs + "] crawling: " + url)
      parseURL(url)
    }
    println((System.currentTimeMillis() - startTime) / 1000)

    /*if (verbose >= 1)
      println("Detecting languages of unique documents...")
    for ((k, v) <- fingerprints) {
      v.foreach {
        x =>
          if (!x.isEmpty && langDet.isEnglish(x))
            uniqEngPages = uniqEngPages + 1
      }
    }*/
    val time = start - System.currentTimeMillis()

    println("Pages per seconds : " + ((uniqueURLs.size - notFoundResources).toDouble / (time / 1000)))

   // val dupli = searchDuplicates()
    println("-----------OUTPUT------------")
    println("Distinct URLs: " + (uniqueURLs.size - notFoundResources) + " login/personen : " + cntloginPerso)
    println("Exact duplicates: " + countED)
    println("Near duplicates : " + countND)
    println("Unique English pages found: " + uniqEngPages)
    println("Term frequency of \"student\": " + studentOccurrences)
  }

  def parseURL(url: URL) {
    val strURL = url.toString
    val urlRegex = "(href=\")([^\"]*)".r
    val textRegex = "(>)([^<>]+[a-zA-Z0-9]+)".r
    val anchorsAndParams = "(#.*)|(\\?.*)".r
    val loginRegex = "(login[a-z0-9]{4})|(personendetail[a-z0-9]{4})".r
    var pageText = ""
    var sourceCode: BufferedSource = null
    try {
      sourceCode = io.Source.fromURL(url)
    } catch {
      case fnfe: FileNotFoundException => {
        if (verbose >= 1)
          System.err.println(" Resource not found: " + url)
        notFoundResources += 1
        return
      }
    }

    for (l <- sourceCode.getLines()) {
      // find new URLS
      urlRegex.findAllIn(l).matchData foreach {
        m =>
          {
            val foundURL = anchorsAndParams.replaceAllIn(m.group(2), "")
            var absoluteFoundURL: URL = null
            try {
              absoluteFoundURL = new URL(url, foundURL)
              val absURL = absoluteFoundURL.toString
              if (absURL.endsWith(".html") && absURL.startsWith(Domain)
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
            } catch {
              case murle: MalformedURLException => {
                if (verbose >= 2) System.err.println(" Invalid url " + foundURL)
              }
            }
          }
      }
      // Get textual content
      textRegex.findAllIn(l).matchData foreach {
        m =>
          {
            val text = m.group(2)
            pageText += text + " "
          }
      }
    }
    pageText = pageText.replaceAll("&nbsp;?", " ").replaceAll("&[Aa]uml;", "ä")
      .replaceAll("&[Oo]uml;", "ö").replaceAll("&[Uu]uml;", "ü")

    val f = pageText.hashCode()
    if (fingerprints.contains(f)) { // exact duplicate
      val collisions = fingerprints.apply(f)
      if (collisions.contains(pageText)) {
        if (verbose >= 1)
          println(" exact duplicate")
        exactDuplicates += 1
        countED += 1
      } else {
        collisions += pageText
      }
    } else { // check for near-duplicates
      fingerprints.put(f, MutableList(pageText))

      if (pageText != "")
        if (langDet.isEnglish(pageText)) uniqEngPages = uniqEngPages + 1

      val shingles = shingle(pageText, q)
      //val hashes = shingles.map(_.hashCode).map { h => binary(h) }.toList
      // check near-duplicate
      /*val binaryHashCodes = shingles.map(x => binary(x.hashCode).toList)

      val minHashes = MutableList[String]()
      for (positions <- permCodes) {
        val permHashes = for (hc <- binaryHashCodes)
          yield positions.map(hc.apply(_))
        minHashes += permHashes.toList.map(_.mkString).min
      }
      for (l <- pageHashesMinHash) {
        val coincide = for (n <- 0 until numRepetitions)
          yield minHashes.apply(n) == l._1.apply(n)
        val sim = coincide.filter(_ == true).length.toDouble / numRepetitions
        if (sim >= nearDuplicateThreshold) {
          nearDuplicates += 1
          println("J(S_1, S_2) = " + sim)
          println(url + " is similar to " + l._2)
        }
      }
      pageHashesMinHash += Tuple2(minHashes.toList, url)*/

      val setHashShingles = shingles.map { _.hashCode() }.toSet

      jaccardHash.put(strURL, setHashShingles)

      val hashesSimHash = setHashShingles.map { h => binary(h) }.toList

      val fingerprint = simHash(hashesSimHash)
      //storePermutation(url, fingerprint)
      countND += searchDuplicatesAndStorePermu(strURL, fingerprint)

      pageHashesSimHash.put(strURL, fingerprint)
    }

    val currentStudent = "(?i)\\sstudent\\s".r.findAllIn(pageText).length
    studentOccurrences += currentStudent
  }

  def generatePermutationCodes(k: Int, n: Int): List[List[Int]] = {
    val until32 = (0 until 32).toList
    val permutationCodes = for (n <- 0 until n)
      yield Random.shuffle(until32)
    return permutationCodes.map(_.takeRight(k)).toList
  }

  def shingle(text: String, q: Int): Set[Shingle] = {
    val tks = text.split("[ .,;:/?!\t\n\r\f]+").toList
    return tks.sliding(q).toSet.asInstanceOf[Set[Shingle]]
  }

  def binary(value: Int): String =
    String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0')

  def sign(n: Int): Int = {
    if (n < 0) return 0
    else return 1
  }

  def simHash(shingleSet: List[String]): List[Int] = {
    val listOfbitList = shingleSet.map { x => x.sliding(1).toList.map(_.toString.toInt) }

    val bigG = for { pos <- 0 until listOfbitList.apply(0).length }
      yield listOfbitList.map { x => (x.apply(pos)) * 2 - 1 }.reduce(_ + _)

    val smallG = bigG.map { x => sign(x) }.toList

    return smallG
  }

  def definePermutations(n: Int, topNBits: Int) {
    var iter = n

    while (iter != 0) {
      val randomP = sample(0 to 31 toList, topNBits)
      if (!permutedTables.contains(randomP)) {
        permutedTables.put(randomP, HashMap[Int, MutSet[String]]())
        iter = iter - 1;
      }
    }
  }

  /*def storePermutation(url: String, fingerprint: List[Int]) {
    val emptyMask = "00000000000000000000000000000000"

    val fpInt = convertBinaryListToInt(fingerprint)

    for (pi_k <- permutedTables.keySet) {
      val maskL = pi_k.foldLeft(emptyMask)((s, i) => s.updated(i, '1')).toList.map(_.toString.toInt)

      val maskInt = convertBinaryListToInt(maskL)

      val permutedFPInt = fpInt & maskInt

      val FPHashtable = permutedTables(pi_k)
      val urlList = FPHashtable.getOrElse(permutedFPInt, MutSet())
      FPHashtable.update(permutedFPInt, urlList += url)
      permutedTables.update(pi_k, FPHashtable)

    }
  }*/

  def sample[A](itms: List[A], sampleSize: Int) = {
    def collect(vect: Vector[A], sampleSize: Int, acc: List[A]): List[A] = {
      if (sampleSize == 0) acc
      else {
        val index = Random.nextInt(vect.size)
        collect(vect.updated(index, vect(0)) tail, sampleSize - 1, vect(index) :: acc)
      }
    }

    collect(itms toVector, sampleSize, Nil)
  }

  def convertBinaryListToInt(binList: List[Int]): Int =
    (0 to 31).foldLeft(0) { (a, index) => a + (1 << index) * binList.reverse(index) }

  def searchDuplicatesAndStorePermu(urlQ: String, fingerprint: List[Int]): Int = {
    var cntND = 0

    val keyList = pageHashesSimHash.keySet.toList
    val emptyMask = "0" * 32

    var candidates = Set[String]()

    val fpInt = convertBinaryListToInt(fingerprint)

    for (pi_k <- permutedTables.keySet) {
      val maskL = pi_k.foldLeft(emptyMask)((s, i) => s.updated(i, '1')).toList.map(_.toString.toInt)
      val maskInt = convertBinaryListToInt(maskL)

      val query = fpInt & maskInt
      candidates = candidates ++ permutedTables(pi_k).getOrElse(query, Set())

      val FPHashtable = permutedTables(pi_k)
      val urlList = FPHashtable.getOrElse(query, MutSet())
      FPHashtable.update(query, urlList += urlQ)
      permutedTables.update(pi_k, FPHashtable)

    }

    println("Number of candidates for " + urlQ + " (" + fpInt + ") : " + candidates.toSet.size)
    val fpCandidates = for { url <- candidates } yield ((pageHashesSimHash.getOrElse(url, List()), url))

    val counts = compareHamming((fingerprint, urlQ), fpCandidates)
    cntND += counts

    return cntND
  }

/*  def searchDuplicates(): Int = {
    var cntND = 0
    var cntED = 0

    val keyList = pageHashesSimHash.keySet.toList
    var countIter = 0
    val emptyMask = "0" * 32

    for (i <- 0 until pageHashesSimHash.keySet.size) {
      var candidates = Set[String]()
      val urlQ = keyList.apply(i)
      val fp = pageHashesSimHash.getOrElse(urlQ, Nil)

      val fpInt = convertBinaryListToInt(fp)

      for (pi_k <- permutedTables.keySet) {
        val maskL = pi_k.foldLeft(emptyMask)((s, i) => s.updated(i, '1')).toList.map(_.toString.toInt)
        val maskInt = convertBinaryListToInt(maskL)

        val query = fpInt & maskInt
        candidates = candidates ++ permutedTables(pi_k).getOrElse(query, Set())
        countIter += 1
      }

      println("Number of candidates for " + urlQ + " (" + fpInt + ") : " + candidates.toSet.size)
      val fpCandidates = for { url <- candidates } yield ((pageHashesSimHash.getOrElse(url, List()), url))

      val counts = compareHamming((fp, urlQ), fpCandidates)
      cntND += counts
    }

    return cntND
  }*/

  def compareHamming(queryFP: (List[Int], String), candidates: Set[(List[Int], String)]): Int = {
    def hammingDistance(s1: String, s2: String): Int = s1.zip(s2).count(c => c._1 != c._2)

    val cSet = candidates

    var cntND = 0;
    var cntED = 0;
    val qStr = queryFP._1.mkString
    val urlQ = queryFP._2

    for { candidate <- cSet } {
      val candidateStr = candidate._1.mkString
      val candidateUrl = candidate._2

      if (!qStr.equals(candidateStr) && !urlQ.equals(candidateUrl)) {
        if (hammingDistance(qStr, candidateStr) <= 6) {
          if (jaccardSimilarity(urlQ, candidateUrl) > 0.85)
            cntND += 1
        }
      }

    }

    return cntND
  }

  def jaccardSimilarity(url1: String, url2: String): Double = {
    val url1Set = jaccardHash.getOrElse(url1, Set())
    val url2Set = jaccardHash.getOrElse(url2, Set())

    val sim = (url1Set intersect url2Set).size.toDouble / (url1Set union url2Set).size

    return sim
  }

  def hammingDistance(s1: String, s2: String): Int = s1.zip(s2).count(c => c._1 != c._2)

}
