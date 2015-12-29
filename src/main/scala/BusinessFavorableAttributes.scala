import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by sidd on 12/20/15.
 */
object BusinessFavorableAttributes {

  def main(args:Array[String]):Unit={

    // Immediately exit if three arguments are not given
    if(args.length < 3){
      throw new IllegalArgumentException("Not enough input arguments provided. [master] [inputFile1] [inputFile2] needed")
      sys.exit(0)
    }

    // Necessary initialization
    val conf = new SparkConf().setMaster(args(0)).setAppName("BusinessFavorableAttributes")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Raw inputs
    val rawBusinessInput = sqlContext.read.json(args(1))
    val rawReviewInput = sqlContext.read.json(args(2))

    val businessReviews = rawReviewInput.map(row => (row(0).toString,row(4).toString)).reduceByKey(_ + " " + _)

    //LDACategorizer
    val competitionGroups = LDACategorizer.adapt(businessReviews,145,50,100)
    val rankedBusinesses = BusinessRanker.adapt(competitionGroups,rawBusinessInput,businessReviews)
    val withFeatures:Array[Array[Array[Tuple2[Double,Double]]]] = BetterFeaturesIdentifier.adapt(rankedBusinesses)



    //BusinessRanker
    //BetterFeaturesIdentifier


  }

}
