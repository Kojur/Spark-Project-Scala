import Analysis._
import org.apache.spark.sql.DataFrame

/**
 * Object responsible for calculating the average number of turns in the dataset.
 * Uses tail recursion for efficient computation and custom error handling.
 */
object CalculateAverageTurns {

  /**
   * Tries to calculate the average number of turns in a DataFrame.
   *
   * Steps:
   * - Selects the "turns" column and removes nulls.
   * - Converts the column to a List[Int].
   * - Applies a tail-recursive function to compute the average.
   *
   * @param df Input DataFrame containing a "turns" column
   * @return SuccessResult(average) or ErrorResult(message) in case of failure
   */
  def tryCalculateAverageTurns(df: DataFrame): Result[Double] =
    try {
      // Extract the "turns" column as a list of integers
      val turnsList: List[Int] = df.select("turns")
        .na.drop() // remove nulls
        .collect() // bring rows to driver
        .map(_.getInt(0)) // extract value from Row
        .toList

      // Compute average using tail recursion
      val avg = avgTailRec(turnsList)
      SuccessResult(avg)
    } catch {
      case e: Exception =>
        // Return custom error on failure
        ErrorResult(s"Avg calculation failed: ${e.getMessage}")
    }
}
