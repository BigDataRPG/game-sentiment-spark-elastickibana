package Boyd.Bigdatarpg.ProjectSSCSentimentAnalysis

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import org.elasticsearch.spark.rdd.EsSpark

/** Listens to a stream of Tweets and keeps track of the most popular
  *  hashtags over a 5 minute window.
  */
object GetSelectedTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {


    /** Initialize the Spark app with Migration Elasticsearch
      **/

    // Initialize a SparkConf with all available cores
    val sparkConf = new SparkConf().setAppName("GetSelectedTweets").setMaster("local[6]")
    // Automatically create index in Elasticsearch
    sparkConf.set("es.resource","sparksender/tweets")
    sparkConf.set("es.index.auto.create", "False")
    // Define the location of Elasticsearch cluster
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // all CPU cores and four-second batches of data
    // Create a StreamingContext with a batch interval of 4 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    /** If you don't use Elasticsearch
      **/
//    val ssc = new StreamingContext("local[6]", "GetSelectedTweets", Seconds(4))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)// .filter(_.getLang() == ("th", "en"))

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map( status => status.getCreatedAt)
    statuses.print

    // val filteredStatuses = statuses.filter(word => word.toLowerCase().contains("apple", "samsung", "huawei"))
    // filteredStatuses.print

    // Create Map Tweet with Multiple Attribute from Tweets
//    val tweetMap = tweets.map(status => { val tweetMap =
//      ("location" -> Option(status.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" })) ~
//      ("UserLocation" -> Option(status.getUser.getLocation)) ~
//      ("UserName" -> status.getUser.getName) ~
//      ("TextLength" -> status.getText.length) ~
//      // Tokenized the tweet message and then filtered only words starting with #
//      ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" ")) ~
////      ("PlaceCountry" -> Option(status.getsPlace).map (pl => {s"${pl.getCountry}"}))
//      ("Text" -> status.getText)
//    })

//    tweetMap.print
//    tweetMap.map(s => List(s)).print
//
    // Each batch is saved to Elasticsearch with StatusCreatedAt as the default time dimension
//    tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparksender/tweets") }




    // Each batch is saved to Elasticsearch
//    filteredStatuses.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparksender/tweets")) }



        // Blow out each word into a new DStream
//    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
//    val filteredTweets = tweetwords.filter( eachWord => eachWord.toLowerCase() contains "#")
//    filteredTweets.print

//
//    tweetwords.print

    //    // Now eliminate anything that's not a hashtag
    //    val hashtags = tweetwords.filter(word => word.toLowerCase.startsWith("#BTS"))

    //
    //    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    //    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    //
    //    // Now count them up over a 5 minute window sliding every one second
    //    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    //    //  You will often see this written in the following shorthand:
    //    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    //
    //    // Sort the results by the count values
    //    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    //
    //    // Print the top 10
    //    sortedResults.print

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("/Users/redthegx/SparkMLProjects/sparkmlprojects/src/main/resources/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
