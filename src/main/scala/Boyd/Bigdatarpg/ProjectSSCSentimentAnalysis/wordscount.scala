package Boyd.Bigdatarpg.ProjectSSCSentimentAnalysis

import org.apache.spark._
import scala.collection.immutable.ListMap
import org.apache.log4j._

/** Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object wordcount {

  def main(args: Array[String]) {
    // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Create a RDD of lines of text in our book
    val input = sc.textFile("/Users/redthegx/SparkMLProjects/sparkmlprojects/src/main/resources/input/book.txt")
    // Use flatMap to convert this into an rdd of each word in each line
    val words = input.flatMap(line => line.split(' '))
    // Convert these words to lowercase
    val lowerCaseWords = words.map(word => word.toLowerCase())
    // Count up the occurence of each unique word
    val wordCounts = lowerCaseWords.countByValue()

    // Print the first 20 results
    val sample = wordCounts.toList.sortWith( (x,y) => x._2 > y._2 )
//    println(sample.take(5))
      //.toSeq.sortWith(_._1 > _._1)

//    val take20 = sample.take(20)
//
    for ((word, count) <- sample.take(30)) {
      println(word + " " + count)
    }

    sc.stop()
  }
}
