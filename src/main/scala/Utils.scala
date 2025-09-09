import org.apache.spark.sql.{DataFrame, SparkSession}

/** Utility functions for data loading and preprocessing */
object Utils {

  /**
   * Attempts to load a CSV file into a DataFrame using a try-catch block.
   * Returns a custom Result type to model success or failure.
   *
   * @param spark The active Spark session
   * @param path Path to the CSV file
   * @return Result wrapping DataFrame or error message
   */
  def tryLoadData(spark: SparkSession, path: String): Result[DataFrame] =
    try {
      val df = loadData(spark, path)
      SuccessResult(df)
    } catch {
      case e: Exception =>
        // Return a custom error result on failure
        ErrorResult(s"Load failed: ${e.getMessage}")
    }

  /**
   * Loads a CSV file into a DataFrame with headers and schema inference.
   *
   * @param spark The Spark session
   * @param path The CSV file path
   * @return The loaded DataFrame
   */
  private def loadData(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  /**
   * Selects a predefined set of numeric columns from the DataFrame.
   * This function returns a transformation lambda.
   *
   * @return Function that selects columns
   */
  def selectNumericColumns: DataFrame => DataFrame = df =>
    df.select(
      "turns",
      "white_rating",
      "black_rating",
      "opening_ply",
      "rating_difference",
      "avg_rating",
      "white_win",
      "is_rated",
      "duration_seconds",
      "increment_base"
    )

  /**
   * Drops rows with null values from the given DataFrame.
   *
   * @param df Input DataFrame
   * @return Cleaned DataFrame with nulls removed
   */
  def filterValidRows(df: DataFrame): DataFrame = df.na.drop()
}
