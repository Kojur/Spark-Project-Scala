import Analysis._
import org.apache.spark.sql.DataFrame

object CalculateAverageTurns {
  def tryCalculateAverageTurns(df: DataFrame): Result[Double] =
    try {
      val turnsList: List[Int] = df.select("turns")
        .na.drop()
        .collect()
        .map(_.getInt(0))
        .toList

      val avg = avgTailRec(turnsList)
      SuccessResult(avg)
    } catch {
      case e: Exception => ErrorResult(s"Avg calculation failed: ${e.getMessage}")
    }
}
