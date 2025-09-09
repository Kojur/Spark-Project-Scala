import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, split, when}

/**
 * Object containing transformation combinators used in the functional pipeline.
 * Each method returns a transformation function that can be applied to a DataFrame.
 */
object Combinator {

  /**
   * Computes additional rating statistics.
   *
   * Adds the following columns:
   * - `rating_difference`: white_rating - black_rating
   * - `avg_rating`: average of white and black ratings
   * - `is_rated`: cast of "rated" to integer
   * @return DataFrame transformation
   */
  def withRatingStats: DataFrame => DataFrame = df =>
    df.withColumn("rating_difference", col("white_rating") - col("black_rating"))
      .withColumn("avg_rating", (col("white_rating") + col("black_rating")) / 2)
      .withColumn("is_rated", col("rated").cast("int"))

  /**
   * Adds a binary indicator of whether white won the game.
   *
   * Creates the `white_win` column:
   * - 1 if winner is "white" (case-insensitive)
   * - 0 otherwise
   *
   * @return DataFrame transformation
   */
  def withGameOutcome: DataFrame => DataFrame = df =>
    df.withColumn("white_win", when(lower(col("winner")) === "white", 1).otherwise(0))

  /**
   * Extracts time-based features.
   *
   * Adds the following columns:
   * - `duration_seconds`: game duration in seconds
   * - `increment_base`: base increment value extracted from "increment_code"
   *
   * @return DataFrame transformation
   */
  def withTimeStats: DataFrame => DataFrame = df =>
    df.withColumn("duration_seconds", (col("last_move_at") - col("created_at")) / 1000)
      .withColumn("increment_base", split(col("increment_code"), "\\+").getItem(0).cast("int"))

  /**
   * Composes multiple DataFrame transformations into one.
   *
   * @param transformations A sequence of DataFrame => DataFrame functions
   * @return A single composed transformation
   */
  def compose(transformations: (DataFrame => DataFrame)*): DataFrame => DataFrame =
    df => transformations.foldLeft(df)((acc, transform) => transform(acc))

}
