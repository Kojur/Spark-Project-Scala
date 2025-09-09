import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.{BitmapEncoder, HeatMapChart, HeatMapChartBuilder, XYChart, XYChartBuilder}

import scala.jdk.CollectionConverters._
import java.io.PrintWriter
import scala.annotation.tailrec

/**
 * Provides statistical analysis utilities for DataFrame operations.
 * Includes feature assembly, correlation calculation, plotting, and
 * tail-recursive aggregation functions for functional programming.
 */
object Analysis {

  /**
   * Transforms a DataFrame into a feature vector column required for MLlib statistics.
   *
   * @param df Input DataFrame with numeric columns
   * @return DataFrame with a new "features" vector column
   */
  def assembleFeatures(df: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(df.columns)
      .setOutputCol("features")
    assembler.transform(df)
  }

  /**
   * Computes the Pearson correlation matrix for the DataFrame's feature vector column.
   *
   * @param df DataFrame with "features" column
   * @return Spark MLlib correlation matrix
   */
  def computeCorrelationMatrix(df: DataFrame): Matrix =
    Correlation.corr(df, "features", "pearson").head().getAs[Matrix]("pearson(features)")

  /**
   * Saves a heatmap of the correlation matrix to a PNG file.
   *
   * @param corrMatrix Correlation matrix
   * @param cols       Array of feature column names
   */
  def saveCorrelationHeatmap(corrMatrix: Matrix, cols: Array[String]): Unit = {
    // Prepare data for heatmap
    val data: java.util.List[Array[Number]] =
      (for {
        i <- 0 until corrMatrix.numRows
        j <- 0 until corrMatrix.numCols
      } yield Array(
        j.asInstanceOf[Number],
        i.asInstanceOf[Number],
        corrMatrix(i, j).asInstanceOf[Number]
      )).toList.asJava

    // Build and save chart
    val chart: HeatMapChart = new HeatMapChartBuilder()
      .width(900)
      .height(700)
      .title("Correlation Heatmap")
      .xAxisTitle("Features")
      .yAxisTitle("Features")
      .build()

    chart.addSeries("correlation", cols.toList.asJava, cols.toList.asJava, data)
    BitmapEncoder.saveBitmap(chart, "correlation_heatmap.png", BitmapFormat.PNG)
  }

  /**
   * Generates scatter plots and histograms for all pairwise combinations of numeric columns.
   * Saves each plot as a PNG file in the `scatterplots/` directory.
   *
   * @param df Cleaned DataFrame with numeric columns only
   */
  def buildScatterPlots(df: DataFrame): Unit = {
    val spark = df.sparkSession
    import spark.implicits._

    val cols = df.columns

    // Convert all columns to Double and drop nulls
    val collected = df
      .select(cols.map(col(_).cast("double")): _*)
      .na.drop()
      .collect()
      .map(row => row.toSeq.map(_.asInstanceOf[Double]).toArray)

    // Create output folder
    val folder = new java.io.File("scatterplots")
    if (!folder.exists()) folder.mkdir()

    // Generate all pairwise charts
    for {
      i <- cols.indices
      j <- cols.indices
    } yield {
      val x = collected.map(_(j))
      val y = collected.map(_(i))

      val chart =
        if (i == j) histogramChart(cols(i), x)
        else scatterChart(cols(j), cols(i), x, y)

      BitmapEncoder.saveBitmap(chart, s"scatterplots/${cols(i)}_vs_${cols(j)}", BitmapFormat.PNG)
    }
  }

  /**
   * Creates a histogram chart from a numeric column.
   *
   * @param label Column name
   * @param data  Numeric array for that column
   * @return XYChart histogram chart
   */
  def histogramChart(label: String, data: Array[Double]): XYChart = {
    val bins = 20
    val (minVal, maxVal) = (data.min, data.max)
    val binWidth = (maxVal - minVal) / bins

    val counts = Array.fill(bins)(0)
    data.foreach { v =>
      val bin = Math.min(((v - minVal) / binWidth).toInt, bins - 1)
      counts(bin) += 1
    }

    val binCenters = Array.tabulate(bins)(b => minVal + b * binWidth + binWidth / 2)

    val chart = new XYChartBuilder()
      .width(600)
      .height(400)
      .title(s"Histogram: $label")
      .xAxisTitle(label)
      .yAxisTitle("Count")
      .build()

    chart.addSeries(label, binCenters, counts.map(_.toDouble))
    chart
  }

  /**
   * Creates a scatter chart between two numeric variables.
   *
   * @param xLabel Label for X-axis
   * @param yLabel Label for Y-axis
   * @param x      Data for X-axis
   * @param y      Data for Y-axis
   * @return XYChart scatter chart
   */
  def scatterChart(xLabel: String, yLabel: String, x: Array[Double], y: Array[Double]): XYChart = {
    val chart = new XYChartBuilder()
      .width(600)
      .height(400)
      .title(s"$xLabel vs $yLabel")
      .xAxisTitle(xLabel)
      .yAxisTitle(yLabel)
      .build()

    chart.addSeries("points", x, y)
    chart
  }

  /**
   * Tail-recursive implementation of average computation over a list of integers.
   *
   * @param data  List of integers
   * @param sum   Accumulator for sum (default: 0)
   * @param count Accumulator for count (default: 0)
   * @return Average value as Double
   */
  @tailrec
  def avgTailRec(data: List[Int], sum: Long = 0, count: Long = 0): Double = {
    data match {
      case Nil => if (count == 0) 0.0 else sum.toDouble / count
      case head :: tail => avgTailRec(tail, sum + head, count + 1)
    }
  }

  /**
   * Saves average turn count to a text file with two decimal formatting.
   *
   * @param avg  Average value
   * @param path Output file path (default: "average_turns.txt")
   */
  def saveAverageToFile(avg: Double, path: String = "average_turns.txt"): Unit = {
    val writer = new PrintWriter(path)
    writer.println(f"Average Turns: $avg%.2f")
    writer.close()
  }
}
