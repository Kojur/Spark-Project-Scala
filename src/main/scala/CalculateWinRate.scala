import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.{BitmapEncoder, HeatMapChart, HeatMapChartBuilder, XYChart, XYChartBuilder}

object CalculateWinRate {
  /**
   * Builds and saves a chart showing White's win rate by average rating bucket.
   *
   * @param df DataFrame with "avg_rating" and "white_win"
   */
  def saveWinRateByRating(df: DataFrame): Unit = {
    val spark = df.sparkSession
    import spark.implicits._

    // Bucketize ratings into ranges of 400
    val bucketed = df.select("avg_rating", "white_win").na.drop()
      .withColumn("rating_bucket", (col("avg_rating") / 400).cast("int") * 400)
      .groupBy("rating_bucket")
      .agg(
        avg(col("white_win")).as("white_win_rate")
      )
      .orderBy("rating_bucket")
      .collect()

    val buckets = bucketed.map(_.getInt(0).toDouble)   // numeric x-axis
    val winRates = bucketed.map(_.getDouble(1) * 100)  // percentage y-axis

    val chart = new XYChartBuilder()
      .width(700)
      .height(450)
      .title("White Win Rate by Rating Bucket")
      .xAxisTitle("Average Rating Bucket")
      .yAxisTitle("White Win Rate (%)")
      .build()

    chart.addSeries("White Win Rate", buckets, winRates)
    BitmapEncoder.saveBitmap(chart, "white_win_rate_by_rating.png", BitmapFormat.PNG)
  }


}
