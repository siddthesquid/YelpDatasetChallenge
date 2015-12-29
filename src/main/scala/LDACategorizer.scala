import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.clustering.{LDA,DistributedLDAModel}
import scala.collection.mutable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * Created by sidd on 11/4/15.
 */
object LDACategorizer {

  def adapt(businessReviews:RDD[(String,String)],numStopwords:Integer,numTopics:Integer,numIterations:Integer)={

    val businesses = businessReviews.zipWithIndex().map({case ((business,review),index)=>(index,business)})

    val tokenized: RDD[Seq[String]] = businessReviews.map(_._2)
      .map(_.toLowerCase.split("\\s"))
      .map(_.filter(_.length > 3)
      .filter(_.forall(java.lang.Character.isLetter)))

    val termCounts: Array[(String, Long)] = tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    val vocabArray: Array[String] = termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }.repartition(5)

    val lda = new LDA().setK(numTopics).setMaxIterations(numIterations)

    val distLDAModel:DistributedLDAModel = lda.run(documents).asInstanceOf[DistributedLDAModel]

    val topicDocument =  distLDAModel.topTopicsPerDocument(1).map({case (index,topics,weights)=>(index,topics(0))})

    val businessGroups = topicDocument.join(businesses).map({case (index,(topic,business))=>(topic,business)})
      .groupByKey()
      .map({case (topic,businessArray)=>businessArray})

    businessGroups.collect()

  }

}
