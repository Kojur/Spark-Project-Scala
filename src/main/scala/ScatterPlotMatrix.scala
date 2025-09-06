import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.knowm.xchart.{XYChart, XYChartBuilder, BitmapEncoder}
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import scala.jdk.CollectionConverters._

object ScatterPlotMatrix {

  def buildScatterPlots(df: DataFrame, columns: Array[String]): Unit = {
    val spark = df.sparkSession
    import spark.implicits._

    // Collect data to driver (CAREFUL: only safe if dataset isn’t huge!)
    val rows = df.select(columns.map(c => col(c).cast("double")): _*)
      .na.drop()
      .collect()

    val collected = rows.map { row =>
      (0 until row.length).map(i => row.getDouble(i)).toSeq
    }

    val data = collected.map(_.toArray)

    // Create folder for plots
    val folder = new java.io.File("scatterplots")
    if (!folder.exists()) folder.mkdir()

    for (i <- columns.indices; j <- columns.indices) {
      val xVals = data.map(_(j))
      val yVals = data.map(_(i))

      val chart =
        if (i == j) {
          // Diagonal → histogram of variable
          val hist = new XYChartBuilder()
            .width(600)
            .height(400)
            .title(s"Histogram: ${columns(i)}")
            .xAxisTitle(columns(i))
            .yAxisTitle("Count")
            .build()

          val bins = 20
          val minVal = xVals.min
          val maxVal = xVals.max
          val binWidth = (maxVal - minVal) / bins
          val histData = Array.fill(bins)(0)
          xVals.foreach { v =>
            val bin = Math.min(((v - minVal) / binWidth).toInt, bins - 1)
            histData(bin) += 1
          }
          val binCenters = (0 until bins).map(b => minVal + b * binWidth + binWidth / 2).toArray
          hist.addSeries(columns(i), binCenters, histData.map(_.toDouble))
          hist
        } else {
          // Off-diagonal → scatter plot
          val scatter = new XYChartBuilder()
            .width(600)
            .height(400)
            .title(s"${columns(j)} vs ${columns(i)}")
            .xAxisTitle(columns(j))
            .yAxisTitle(columns(i))
            .build()

          scatter.addSeries("points", xVals.toArray, yVals.toArray)
          scatter
        }

      BitmapEncoder.saveBitmap(chart, s"scatterplots/${columns(i)}_vs_${columns(j)}", BitmapFormat.PNG)
    }

    println("Scatterplot matrix saved in folder: scatterplots/")
  }
}