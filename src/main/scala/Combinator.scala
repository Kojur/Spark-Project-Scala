import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, split, when}

object Combinator {

  def withRatingStats: DataFrame => DataFrame = df =>
    df.withColumn("rating_difference", col("white_rating") - col("black_rating"))
      .withColumn("avg_rating", (col("white_rating") + col("black_rating")) / 2)

  def withGameOutcome: DataFrame => DataFrame = df =>
    df.withColumn("white_win", when(lower(col("winner")) === "white", 1).otherwise(0))

  def withTimeStats: DataFrame => DataFrame = df =>
    df.withColumn("is_rated", col("rated").cast("int"))
      .withColumn("duration_seconds", (col("last_move_at") - col("created_at")) / 1000)
      .withColumn("increment_base", split(col("increment_code"), "\\+").getItem(0).cast("int"))

  def compose(transformations: (DataFrame => DataFrame)*): DataFrame => DataFrame =
    df => transformations.foldLeft(df)((acc, transform) => transform(acc))



}
