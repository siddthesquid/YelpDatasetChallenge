import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Created by sidd on 12/20/15.
 */
object BusinessRanker {

  def adapt(competitionGroups:Array[Iterable[String]],rawBusinessInput:DataFrame,businessReviews:RDD[(String,String)]) = {

    val businessScores = rawBusinessInput.map(row=>(row(1).toString,row(12).toString.toDouble))

    competitionGroups.map(_.map(business => (business,businessScores.lookup(business)(0))))

  }

}
