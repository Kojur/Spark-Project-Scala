import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def tryLoadData(spark: SparkSession, path: String): Result[DataFrame] =
    try {
      val df = loadData(spark, path)
      SuccessResult(df)
    } catch {
      case e: Exception => ErrorResult(s"Load failed: ${e.getMessage}")
    }

  def loadData(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  def selectNumericColumns: DataFrame => DataFrame = df =>
    df.select(
      "turns", "white_rating", "black_rating", "opening_ply",
      "rating_difference", "avg_rating", "white_win", "is_rated",
      "duration_seconds", "increment_base"
    )

  def filterValidRows(df: DataFrame): DataFrame = df.na.drop()

}
