package me.cllu.benchmark

import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
  * Spark version
  */
class WikidataExtractorSpark(inputPath: String)(implicit val sc: SparkContext) {
  private val jsons = sc.textFile(inputPath)
    .filter(line => line != "[" && line != "]")
    .map { line =>
      if (line.endsWith(",")) {
        line.substring(0, line.length - 1)
      } else {
        line
      }
    }

  def extractEnglishLabels(outputPath: String): Unit = {
    jsons
      .map { line => new JSONObject(line) }
      .map(json => {
        val labels = json.getJSONObject("labels")
        if (!labels.isNull("en")) {
          json.get("id") + "\t" + labels.getJSONObject("en").getString("value")
        } else {
          null
        }
      })
      .filter(line => line != null)
      .saveAsTextFile(outputPath)
  }
}

object WikidataExtractorSpark {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val master = if (args.length > 1) args(1) else "local[4]"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("Wikidata Extractor")
      .set("spark.executor.memory", "20G")

    implicit val sc: SparkContext = new SparkContext(conf)

    val parser = new WikidataExtractorSpark(inputPath)
    parser.extractEnglishLabels("./data/labels-spark.txt")
  }
}
