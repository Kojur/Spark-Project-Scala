import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix

import org.knowm.xchart.{HeatMapChart, HeatMapChartBuilder, BitmapEncoder}
import scala.jdk.CollectionConverters._

object CorrelationAnalysisScatterPlot {

  def correlationAnalysisScatterPlot(df: DataFrame): DataFrame = {  // Changed to return DataFrame

    val featuredDF = df
      .withColumn("rating_difference", col("white_rating") - col("black_rating"))
      .withColumn("avg_rating", (col("white_rating") + col("black_rating")) / 2)
      .withColumn("white_win", when(lower(col("winner")) === "white", 1).otherwise(0))
      .withColumn("is_rated", col("rated").cast("integer"))
      .withColumn("duration_seconds", (col("last_move_at") - col("created_at")) / 1000)
      .withColumn("increment_base", split(col("increment_code"), "\\+").getItem(0).cast("integer"))

    // Columns to correlate (all numeric)
    val columnsToCorrelate = Array(
      "turns", "white_rating", "black_rating", "opening_ply",
      "rating_difference", "avg_rating", "white_win", "is_rated",
      "duration_seconds", "increment_base"
    )

    val dfForCorr = featuredDF.select(columnsToCorrelate.map(col): _*).na.drop()

    val assembler = new VectorAssembler()
      .setInputCols(columnsToCorrelate)
      .setOutputCol("features")

    val assembledDF = assembler.transform(dfForCorr)

    // Pearson correlation matrix
    val correlationMatrixRow = Correlation.corr(assembledDF, "features", "pearson").head()
    val corrMatrix: Matrix = correlationMatrixRow.getAs[Matrix]("pearson(features)")

    println(s"Pearson correlation matrix:\n ${corrMatrix}")

    // Convert to 2D array
    val corrArray = Array.tabulate(corrMatrix.numRows, corrMatrix.numCols) {
      (i, j) => corrMatrix(i, j)
    }

    // X & Y labels (column names)
    val xLabels = columnsToCorrelate.toList.asJava
    val yLabels = columnsToCorrelate.toList.asJava

    // Heatmap data -> list of Array[Number]
    val data = corrArray.zipWithIndex.flatMap { case (row, i) =>
      row.zipWithIndex.map { case (v, j) =>
        Array(j.asInstanceOf[Number], i.asInstanceOf[Number], v.asInstanceOf[Number])
      }
    }.toList.asJava

    // Build heatmap chart
    val chart: HeatMapChart = new HeatMapChartBuilder()
      .width(900)
      .height(700)
      .title("Correlation Heatmap")
      .xAxisTitle("Features")
      .yAxisTitle("Features")
      .build()

    chart.addSeries("correlation", xLabels, yLabels, data)

    // Save heatmap as PNG
    BitmapEncoder.saveBitmap(chart, "./correlation_heatmap.png", BitmapEncoder.BitmapFormat.PNG)

    println("Correlation heatmap saved to correlation_heatmap.png")

    dfForCorr  // Return the numeric DataFrame for reuse
  }
}