import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import twitter4j.internal.json.StatusJSONImpl
import java.util.Date
import scala.io.Source
import java.io._
import java.io.PrintWriter
import java.io.File;


/**
 * Use this singleton to get or register a Broadcast variable for +ve Words.
 */

object PositiveWord {
  
  @volatile private var instance: Broadcast[Array[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Array[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val filename = "./bin/pos-words.txt"
          instance = sc.broadcast(Source.fromFile(filename).getLines().toArray)
        }
      }
    }
    instance
  }
  
}

/**
 * Use this singleton to get or register a Broadcast variable for -ve Words.
 */

object NegativeWord {
  
  @volatile private var instance: Broadcast[Array[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Array[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val filename = "./bin/neg-words.txt"
          instance = sc.broadcast(Source.fromFile(filename).getLines().toArray)
        }
      }
    }
    instance
  }
  
}


/**
 * Use this singleton to get or register a Broadcast variable for Stop Words.
 */

object StopWord {
  
  @volatile private var instance: Broadcast[Array[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Array[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val filename = "./bin/stop-words.txt"
          instance = sc.broadcast(Source.fromFile(filename).getLines().toArray)
        }
      }
    }
    instance
  }
  
}


/**
 * Use this singleton to get or register an Accumulator to count +ve tweets.
 */

object PositiveTweetCounter {
  
  @volatile private var instance: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0L, "PositiveTweetCounter")
        }
      }
    }
    instance
  }
  
}


/**
 * Use this singleton to get or register an Accumulator to count -ve tweets.
 */

object NegativeTweetCounter {
  
  @volatile private var instance: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0L, "NegetiveTweetCounter")
        }
      }
    }
    instance
  }
  
}

/**
 * Use this singleton to get or register an Accumulator to count -ve tweets.
 */

object NeutralTweetCounter {
  
  @volatile private var instance: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0L, "NeutralTweetCounter")
        }
      }
    }
    instance
  }
  
}

/**
 * 
 * Use this as starting point for performing Tsentiment analysis on tweets from Twitter
 *
 *  To run this app
 *   sbt package
 *   $SPARK_HOME/bin/spark-submit --class "TwitterStreamingApp" --master local[*] ./target/scala-2.10/twitter-streaming-assembly-1.0.jar
 *      <consumer key> <consumer secret> <access token> <access token secret>
 */
object TwitterStreamingApp {
  def main(args: Array[String]) {
    
    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

  	 val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    
    //val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    // to run this application inside an IDE, comment out previous line and uncomment line below
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
    
    //I needed a spark context(sc) to read from text files; so created one with the Spark config
    //passed the spark context(sc)  to create the Spark Streaming Context (ssc)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    //creating Dstream for every 10 sec window for 10 seconds worth of tweets
    val enTweets10 =  stream.filter ( status => status.getUser.getLang.equals("en")).window(Seconds(10), Seconds(10))
    //enTweets10.print()
    //val enTweets30 =  stream.filter ( status => status.getUser.getLang.equals("en")).window(Seconds(30), Seconds(30))
    
    //Loading the +ve, -ve & stop words.Is there a way to use the short path?; 
    //path = "/Users/amarroy/Documents/workspace/twitter-streaming/bin"
    //val positiveWords = sc.textFile("./bin/pos-words.txt").flatMap(l => l.split("\n"))
    //val negativeWords = sc.textFile("./bin/neg-words.txt").flatMap(l => l.split("\n"))
    //val stopWords = sc.textFile("./bin/stop-words.txt").flatMap(l => l.split("\n"))
    
     //loop through the RDDs for the Dstream and parse the twitter text for Sentiment
    enTweets10.foreachRDD(rdd => {
    //enTweets30.foreachRDD(rdd => {
      
      // Get or register the Broadcast variables
      // The broadcast variable approach didn't worked
      val stopWord = StopWord.getInstance(rdd.sparkContext)
      val positiveWord = PositiveWord.getInstance(rdd.sparkContext)
      val negativeWord = NegativeWord.getInstance(rdd.sparkContext)
      
      // Get or register the tweet sentiment Accumulator
      val positiveTweetCounter = PositiveTweetCounter.getInstance(rdd.sparkContext)
      val negativeTweetCounter = NegativeTweetCounter.getInstance(rdd.sparkContext)
      val neutralTweetCounter = NeutralTweetCounter.getInstance(rdd.sparkContext)

      rdd.foreach(status => {
        
        val text = status.getText().replace(".", "").replace(",", "").toLowerCase().split("\\s+").filter(!stopWord.value.contains(_)).filter(!_.isEmpty).distinct
        //text.map(word => println(word));

       //Increment accumulator variables as per the this particular tweet sentiment
       if (tweetSentiment(positiveWord,negativeWord,text)=="positive")
        {
          positiveTweetCounter+=1;
        }
        else if (tweetSentiment(positiveWord,negativeWord,text)=="negative")
        {
          negativeTweetCounter+=1;
        }
        else
        {
          neutralTweetCounter +=1;
        }
        
      })
       
      //print Twitter sentiment in last 10 Sec
      printSentiment(positiveTweetCounter.value.toInt, negativeTweetCounter.value.toInt, neutralTweetCounter.value.toInt, 10)
     
      //print Twitter sentiment in last 30 Sec
      //printSentiment(positiveTweetCounter.value.toInt, negativeTweetCounter.value.toInt, neutralTweetCounter.value.toInt, 30)
     
      
      //set all accumulators to 0
      positiveTweetCounter.value = 0;
      negativeTweetCounter.value = 0;
      neutralTweetCounter.value =0;
    })
   
    //--------------------------------------------------------------------
    //This is the block of code was sample provided by Hien so commenting
    //--------------------------------------------------------------------
    /*val hashTags = stream.flatMap(status => {
      val lang = status.getUser().getLang()
      if (lang == "en") {
        printTweet(lang, status.getText, status.getCreatedAt)
      }
      
      status.getText.split(" ").filter(_.startsWith("#"))
    })*/

    /*val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))*/

    /*val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))*/

    // Print popular hashtags
    /*topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })*/

    /*topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })*/

    ssc.start()
    ssc.awaitTermination()
  }
  
  def printTweet(lang : String, text: String, createdAt: Date) {
    println(s"$createdAt\t$lang\t$text")
  }
  
  //Print sentiment score
  def printSentiment(positive : Int, negative: Int, neutral: Int, duration: Int) {
    println(s"In last $duration sec Twitter Sentiment Score :: Positive:$positive\tNegative:$negative\tNeutral:$neutral")
  }
  
  //classify a particular tweet as +ve, -ve or neutral 
  //def tweetSentiment(positiveWord : RDD[String],negativeWord : RDD[String],tweetWord : Array[String],sc : SparkContext ) : String ={
  def tweetSentiment(positiveWord : Broadcast[Array[String]],negativeWord : Broadcast[Array[String]],tweetWord : Array[String] ) : String ={
  
    
     var countPositiveWord = 0
     var countNegativeWord = 0
     var countNeutralWord = 0
     
     for ( word <- tweetWord) {
          
           //println("tweet words:" + word.toString());
           //println(positiveWord.value);
       
          if(positiveWord.value.contains(word))
              {
                //println("positive word:" + word.toString());
                countPositiveWord +=1;
              }
          else if(negativeWord.value.contains(word))
          {
                //println("negative word:" + word.toString());
                countNegativeWord +=1
          }
          else
          {
                countNeutralWord +=1
          }
              
        }
   
    //Assign sentiment score for the tweet i.e. positive, negative & neutral
    // Is there a better logic?
    if (countPositiveWord > countNegativeWord)
        {
          return "positive" ;
        }
        else if (countPositiveWord  < countNegativeWord)
        {
          return "negative";
        }
        else
        {
           return "neutral";
        }
 
  }
  
  //Write Twitter Sentiment Score output to a file for the given interval i.e. 10 sec or 30 sec
  //For some reason File & PrintWriter is not getting created
  def writeOutput(positive : Int, negative: Int, neutral: Int, duration: Int) {
    
    //Sample Code
    //File file = new File("C:/Users/Me/Desktop/directory/file.txt");
    //PrintWriter printWriter = new PrintWriter ("file.txt");
    //printWriter.println ("hello");
    //printWriter.close ();  
    
  }
}

