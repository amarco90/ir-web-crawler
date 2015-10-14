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
  var domain = ""
  val toParse = new Queue[URL] // Frontier of URLs to parse
  val uniqueURLs = MutSet[URL]() // Unique URLs found
  var notFoundResources = 0 // URLs that give loading error

  val jaccardSimThres = 0.85
  val hammingDistThres = 6
  val numRepetitions = 90
  val numberSimHashPermu = 8
  val topNBitsSimHash = 10

  val fingerprints = new HashMap[Int, MutableList[String]] // for exact dup

  val jaccardHash = HashMap[String, Set[Int]]()
  val pageHashesSimHash = new HashMap[String, List[Int]]
  var permutedTables = HashMap[List[Int], HashMap[Int, MutSet[String]]]()

  val pageHashesMinHash = MutableList[Tuple2[List[String], String]]()
  val permCodes = generatePermutationCodes(32, numRepetitions)

  val q = 5 // q-grams length
  val GramLength = 3 // For language detection
  var langDet: LanguageDetector = null

  // Statistics
  var uniqEngPages = 0
  var countED = 0 // count Exact Duplicates
  var countND = 0 // count Near Duplicates
  var startTime = 0L
  var studentOccurrences = 0
  var visitedURLs = 0

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

    domain = initURL.getProtocol + "://" + initURL.getHost

    langDet = new LanguageDetector(GramLength, verbose)

    definePermutations(numberSimHashPermu, topNBitsSimHash);

    toParse.enqueue(initURL)
    uniqueURLs.add(initURL)

    startTime = System.currentTimeMillis()
    while (!toParse.isEmpty) {
      val url = toParse.dequeue()
      visitedURLs +=1
      if (verbose >= 1)
        println("[" + visitedURLs + "] crawling: " + url)
      parseURL(url)
    }
    printOutput()
  }

  def binary(value: Int): String =
    String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0')

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
        m => {
          val foundURL = anchorsAndParams.replaceAllIn(m.group(2), "")
          var absoluteFoundURL: URL = null
          try {
            absoluteFoundURL = new URL(url, foundURL)
            val absURL = absoluteFoundURL.toString
            if (absURL.endsWith(".html") && absURL.startsWith(domain)
              && !uniqueURLs.contains(absoluteFoundURL)) {
              toParse.enqueue(absoluteFoundURL)
              uniqueURLs.add(absoluteFoundURL)
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
        countED += 1
      } else {
        collisions += pageText
      }
    } else { // check for near-duplicates
      fingerprints.put(f, MutableList(pageText))

      val shingles = shingle(pageText, q)
      //val hashes = shingles.map(_.hashCode).map { h => binary(h) }.toList
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
          countND += 1
          println("J(S_1, S_2) = " + sim)
          println(url + " is similar to " + l._2)
        }
      }
      pageHashesMinHash += Tuple2(minHashes.toList, url)*/

      val setHashShingles = shingles.map { _.hashCode() }.toSet
      jaccardHash.put(strURL, setHashShingles)
      val hashesSimHash = setHashShingles.map { h => binary(h) }.toList
      val fingerprint = simHash(hashesSimHash)
      val currentND = searchDuplicatesAndStorePermu(strURL, fingerprint)
      countND += currentND
      if (currentND == 0) {
        if (pageText != "")
          if (langDet.isEnglish(pageText)) {
            uniqEngPages = uniqEngPages + 1
            studentOccurrences += "(?i)\\sstudent\\s".r.findAllIn(pageText).length
          }
      }
      pageHashesSimHash.put(strURL, fingerprint)
    }
  }

  // For MinHash
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

  def sign(n: Int): Int = {
    if (n < 0) return 0
    else return 1
  }

  def simHash(shingleSet: List[String]): List[Int] = {
    val listOfbitList = shingleSet.map { x => x.sliding(1).toList.map(_.toString.toInt) }
    val bigG = for { pos <- 0 until listOfbitList.apply(0).length }
      yield listOfbitList.map { x => (x.apply(pos)) * 2 - 1 }.reduce(_ + _)

    bigG.map { x => sign(x) }.toList
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

    if (verbose >= 2)
      println("Number of candidates for " + urlQ + " (" + fpInt + "): "
              + candidates.toSet.size)

    val fpCandidates = for { url <- candidates }
      yield ((pageHashesSimHash.getOrElse(url, List()), url))

    val counts = compareHamming((fingerprint, urlQ), fpCandidates)
    cntND += counts
    return cntND
  }

  def compareHamming(queryFP: (List[Int], String), candidates: Set[(List[Int], String)]): Int = {
    def hammingDistance(s1: String, s2: String):
      Int = s1.zip(s2).count(c => c._1 != c._2)

    val cSet = candidates
    var cntND = 0;
    val qStr = queryFP._1.mkString
    val urlQ = queryFP._2

    for { candidate <- cSet } {
      val candidateStr = candidate._1.mkString
      val candidateUrl = candidate._2

      if (!qStr.equals(candidateStr) && !urlQ.equals(candidateUrl)) {
        if (hammingDistance(qStr, candidateStr) <= hammingDistThres
            && jaccardSimilarity(urlQ, candidateUrl) > jaccardSimThres) {
          cntND += 1
        }
      }
    }
    return cntND
  }

  def jaccardSimilarity(url1: String, url2: String): Double = {
    val url1Set = jaccardHash.getOrElse(url1, Set())
    val url2Set = jaccardHash.getOrElse(url2, Set())
    (url1Set intersect url2Set).size.toDouble / (url1Set union url2Set).size
  }

  def hammingDistance(s1: String, s2: String): Int =
    s1.zip(s2).count(c => c._1 != c._2)

  def printOutput() {
    println("Distinct URLs: " + (uniqueURLs.size - notFoundResources))
    println("Exact duplicates: " + countED)
    println("Near duplicates : " + countND)
    println("Unique English pages found: " + uniqEngPages)
    println("Term frequency of \"student\": " + studentOccurrences)
    if (verbose >= 1)
      println("Running time (min): " + 
              (System.currentTimeMillis() - startTime) / 1000 / 60)
  }
}
