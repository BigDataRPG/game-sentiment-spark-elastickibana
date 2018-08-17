package Boyd.Bigdatarpg.ProjectSSCSentimentAnalysis


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import org.apache.spark
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.elasticsearch.spark.sql.EsSparkSQL

/** Listens to a stream of Tweets and keeps track of the most popular
  *  hashtags over a 5 minute window.
  */
object SentimentAnalysis {

  // Turn Sentiment String to Sentiment Score Range [0 to 6]
  def createSentimentScore(s: String): Double = s match {
    case s if s == "NOT_UNDERSTOOD" => 0.0
    case s if s == "VERY_NEGATIVE" => 1.0
    case s if s == "NEGATIVE" => 2.0
    case s if s == "NEUTRAL" => 3.0
    case s if s == "POSITIVE" => 4.0
    case s if s == "VERY_POSITIVE" => 5.0
    case s if s == "NOT_UNDERSTOOD" => 6.0
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // From Spark 2.0.0 to onward use SparkSession (managing Context)
    val spark = SparkSession.builder.appName("SentimentAnalysis").config("spark.master", "local[6]").getOrCreate()
    val sc = spark.sparkContext
    // Create Spark Streaming Context
    val ssc = new StreamingContext(sc, Seconds(10))

    /** Initialize the Spark app with Migration Elasticsearch
      * */
//    val sparkConf = new SparkConf()
//      .setAppName("SentimentAnalysis")
//      .setMaster("local[6]")

//    val sc = new SparkContext(sparkConf)

    // From Spark 2.0.0 to onward use SparkSession (managing Context)
//    val spark = SparkSession
//      .builder()
//      .appName("SentimentAnalysis")
//      .getOrCreate()


    // From Spark 2.0.0 to onward use SparkSession (managing Context)
//    val spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
//    val sc = spark.sparkContext
//    // Create Spark Streaming Context
//    val ssc = new StreamingContext(sc, Seconds(10))


    // Initialize a SparkConf with all available cores
    // Automatically create index in Elasticsearch
    spark.conf.set("es.resource", "sparksender/tweets")
    spark.conf.set("es.index.auto.create", "True")
    // Define the location of Elasticsearch cluster
    spark.conf.set("es.nodes", "localhost")
    spark.conf.set("es.port", "9200")

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    // Create Spark Streaming Context
    // val ssc = new StreamingContext(sparkConf, Seconds(4))


    // Get rid of log spam (should be called after the context is set up)
    setupLogging()
    // Create a DStream from Twitter using our streaming context
    // val stream = TwitterUtils.createStream(ssc, None)

    val filtersMOBA = Seq("moba", "rov", "dota2", "e-sport", "esport", "dota",
      "lol", "Minions", "Demigod", "AvalonHeroes", "HeroesofNewerth", "hon",
    "ArenaofValor", "BrawlStars", "CallofChampions", "HeroesEvolved", "HeroesofOrder",
      "MobileLegends", "IronLeague", "PlanetofHeroes", "Vainglory", "overwatch")

//    val filtersFPS = Seq("FPSgames", "FPS", "HonorableMentions", "e-sport", "esport", "TheDarkness2",
//      "CallofDuty", "LawBreakers", "FarCry", "StarWarsBattlefront", "Counter-Strike", "BioshockInfinite",
//      "Borderlands", "RainbowSixSiege", "Battlefield", "Superhot", "Halo", "Titanfall", "DOOM",
//      "Left4Dead", "Wolfenstein", "Bulletstorm", "Battlefield", "Half-Life", "CriticalOps",
//      "Dead Effect 2","Guns of Boom","pubg","Hitman","IntotheDead2","MADFINGER","ModernCombat5",
//      "Morphite","NOVA")

    val twitterStream = TwitterUtils.createStream(ssc,None,filtersMOBA)
    // val twitterStream = TwitterUtils.createStream(ssc,None)
    val englishTweets = twitterStream.filter(_.getLang == "en")
    // englishTweets.map(_.getText).print()

    // DStream Created using Stanford NLP Library
    // English Model Used (stanford-corenlp-3.9.1-models-english.jar)
    val dataDS = englishTweets.map { tweet =>
      val lengthTweet = tweet.getText.length
      val sentiment = NLPManager.detectSentiment(tweet.getText)
      val tags = tweet.getHashtagEntities.map(_.getText.toLowerCase)
      // val hashtags = tweet.getText.split(" ").filter(word => word.toLowerCase.startsWith("#")).mkString(" ")
      val scoreSentiment = createSentimentScore(NLPManager.detectSentiment(tweet.getText).toString)
//      val lengthDescription = tweet.getUser.getDescription.length
//      val sentimentDescription = NLPManager.detectSentiment(tweet.getUser.getDescription)
//      val scoreSentimentDescription = createSentimentScore(NLPManager.detectSentiment(tweet.getUser.getDescription).toString)
      // val country = tweet.getPlace.getCountry
      // val latitude = tweet.getGeoLocation.getLatitude
      // val longitude = tweet.getGeoLocation.getLongitude
      // println("(" + tweet.getText + " | " + sentiment.toString + " | " + tags)
      (tweet.getCreatedAt.toString,
        tweet.getText,
        lengthTweet,
        tags,
        sentiment.toString,
        scoreSentiment)
        // tweet.getUser.getLocation,
        // tweet.getUser.getDescription,
        // lengthDescription,
        // sentimentDescription.toString,
        // scoreSentimentDescription)
//      ("textTweets" -> tweet.getText,
//        "lengthTweet" -> lengthTweet,
//        "tags" -> tags,
//        "sentiment" -> sentiment.toString,
//        "sentimentScores" -> scoreSentiment,
//        "location" -> tweet.getUser.getLocation,
//        "description" -> tweet.getUser.getDescription,
//        "DesLength" -> lengthDescription,
//        "sentimentDes" -> sentimentDescription.toString,
//        "sentimentDesScores" -> scoreSentimentDescription)
    }
    // dataDS.cache().foreachRDD{ rdd => rdd.foreach(println)}


    val sqlContext = spark.sqlContext

    //    var dataRDD : org.apache.spark.rdd.RDD[(String,String,Array[String])] = sc.emptyRDD
    dataDS.cache().foreachRDD(rdd => {
      val df = spark.createDataFrame(rdd)
      val newNames = Seq("timestamp",
        "textTweets",
        "lengthTweet",
        "tags",
        "sentiment",
        "sentimentScores")
//        "location",
//        "description",
//        "DesLength",
//        "sentimentDes",
//        "sentimentDesScores")
      val dfRenamed = df.toDF(newNames: _*)
      dfRenamed.show()
      EsSparkSQL.saveToEs(dfRenamed, "sparksender/tweets")
       //df.createOrReplaceTempView("sentiments")
       // sqlContext.sql("select * from sentiments limit 20").show()
       // Combine RDDs
        // dataRDD.union(rdd)
    })

    /*
    // Convert DataFrame into SQL Table
    val df = spark.createDataFrame(dataRDD)
    df.show()
    df.createOrReplaceTempView("sentiments")
    sqlContex.sql("select * from sentiments limit 30").show()
    */
    // dataDS.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparksender/tweets") }

    ssc.checkpoint("/Users/redthegx/SparkMLProjects/sparkmlprojects/src/main/resources/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}