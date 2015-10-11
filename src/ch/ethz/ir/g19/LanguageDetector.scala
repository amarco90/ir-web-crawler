package ch.ethz.ir.g19

import java.io._
import collection.mutable.Map

/**
 * @param n Integer the gram length
 * @param verbose Boolean verbose flag
 */
class LanguageDetector(val n : Integer, val verbose : Integer) {
  val englishModel = loadModel("models/english")
  val germanModel = loadModel("models/german")

  def grams(text : String, n : Integer) : Iterator[String] = {
    val normalizedText = text.toLowerCase.replaceAll("[\\d()\"]+", "")
    return normalizedText.sliding(n)
  }

  def isEnglish(query : String) : Boolean = {
    val pEnglish = probability(this.grams(query, n), englishModel)
    val pGerman = probability(this.grams(query, n), germanModel)
    if (verbose >= 1) {
      println("P(english) =" + pEnglish)
      println("P(german)  = " + pGerman)
    }
    return pEnglish > pGerman
  }

  def loadModel(modelPath : String) : Map[String, Double] = {
    val ois = new ObjectInputStream(new FileInputStream(modelPath))
    val model = ois.readObject()
    ois.close()
    return model.asInstanceOf[Map[String, Double]]
  }

  def probability(grams : Iterator[String], langModel : Map[String, Double])
                  : Double = {
    return grams.map(t => Math.log(langModel.getOrElse(t, 0.0)
                .asInstanceOf[Double] + 1)).reduce(_ + _)
  }
}