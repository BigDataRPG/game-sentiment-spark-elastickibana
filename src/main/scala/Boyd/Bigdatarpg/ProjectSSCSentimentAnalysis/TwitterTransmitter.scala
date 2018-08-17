package Boyd.Bigdatarpg.ProjectSSCSentimentAnalysis

import Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}

object TwitterTransmitter {

  def main(args: Array[String]) {

    /** Initialize the Spark app with Migration Elasticsearch
      **/

    // Initialize a SparkConf with all available cores
    val sparkConf = new SparkConf().setAppName("TwitterTransmitter").setMaster("local[6]")
    // Automatically create index in Elasticsearch
    sparkConf.set("es.resource","sparksender/tweets")
    sparkConf.set("es.index.auto.create", "True")
    // Define the location of Elasticsearch cluster
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Accept Twitter Stream filters from arguments passed while running the job
//    val filters = if (args.length == 0) List() else args.toList

    // all CPU cores and four-second batches of data
    // Create a StreamingContext with a batch interval of 4 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream the gets streaming data from Twitter with the filters provided
    val stream = TwitterUtils.createStream(ssc, None).filter(_.getLang() == "th")

    // Process each tweet in a batch
    val tweetMap = stream.map(status => {

      // Defined a DateFormat to convert date Time provided by Twitter to a format understandable by Elasticsearch

      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")

//      def checkObj (x : Any) : String  = {
//        x match {
//          case i:String => i
//          case _ => ""
//        }
//      }


      // Creating a JSON using json4s.JSONDSL with fields from the tweet and calculated fields
      val tweetMap =
        ("UserID" -> status.getUser.getId) ~
          ("UserDescription" -> status.getUser.getDescription) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount) ~
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~
          // Ration is calculated as the number of followers divided by number of people followed
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~
          ("UserLang" -> status.getUser.getLang) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserVerification" -> status.getUser.isVerified) ~
          ("UserName" -> status.getUser.getName) ~
          ("UserStatusCount" -> status.getUser.getStatusesCount) ~
          // User Created DateTime is first converted to epoch miliseconds and then converted to the DateFormat defined above
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText) ~
          ("TextLength" -> status.getText.length) ~
          //Tokenized the tweet message and then filtered only words starting with #
          ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" "))
//          ("PlaceName" -> checkObj(status.getPlace.getName)) ~
//          ("PlaceCountry" -> checkObj(status.getPlace.getCountry))

//      // This function takes Map of tweet data and returns true if the message is not a spam
//      def spamDetector(tweet: Map[String, Any]): Boolean = {
//        {
//          // Remove recently created users = Remove Twitter users who's profile was created less than a day ago
//          Days.daysBetween(new DateTime(formatter.parse(tweet.get("UserCreated").mkString).getTime),
//            DateTime.now).getDays > 1
//        } & {
//          // Users That Create Little Content =  Remove users who have only ever created less than 50 tweets
//          tweet.get("UserStatusCount").mkString.toInt > 50
//        } & {
//          // Remove Users With Few Followers
//          tweet.get("UserFollowersRatio").mkString.toFloat > 0.01
//        } & {
//          // Remove Users With Short Descriptions
//          tweet.get("UserDescription").mkString.length > 20
//        } & {
//          // Remove messages with a Large Numbers Of HashTags
//          tweet.get("Text").mkString.split(" ").filter(_.startsWith("#")).length < 5
//        } & {
//          // Remove Messages with Short Content Length
//          tweet.get("TextLength").mkString.toInt > 20
//        } & {
//          // Remove Messages Requesting Retweets & Follows
//          val filters = List("rt and follow", "rt & follow", "rt+follow", "follow and rt", "follow & rt", "follow+rt")
//          !filters.exists(tweet.get("Text").mkString.toLowerCase.contains)
//        }
//      }
//      // If the tweet passed through all the tests in SpamDetector Spam indicator = FALSE else TRUE
//      spamDetector(tweetMap.values) match {
//        case true => tweetMap.values.+("Spam" -> false)
//        case _ => tweetMap.values.+("Spam" -> true)
//      }
//
    })


    tweetMap.map(s => List(s)).print

    // Each batch is saved to Elasticsearch with StatusCreatedAt as the default time dimension
//    tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparksender/tweets") }


    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("/Users/redthegx/SparkMLProjects/sparkmlprojects/src/main/resources/checkpoint/")
    ssc.start  // Start the computation
    ssc.awaitTermination  // Wait for the computation to terminate
  }
}



















