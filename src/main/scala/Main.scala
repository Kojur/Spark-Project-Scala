// Functional Data Processing Pipeline
// Rewritten for functional programming project compliance with custom combinator `withStats`

import Analysis._
import Utils._
import Combinator._
import CalculateAverageTurns._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix
import org.knowm.xchart.{BitmapEncoder, HeatMapChart, HeatMapChartBuilder, XYChart, XYChartBuilder}
import org.knowm.xchart.BitmapEncoder.BitmapFormat

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import java.io.PrintWriter

// Custom functional error handling
sealed trait Result[+A]
case class SuccessResult[A](value: A) extends Result[A]
case class ErrorResult(message: String) extends Result[Nothing]

case class GameStats(
                      turns: Int,
                      white_rating: Int,
                      black_rating: Int,
                      opening_ply: Int,
                      rating_difference: Int,
                      avg_rating: Double,
                      white_win: Int,
                      is_rated: Int,
                      duration_seconds: Long,
                      increment_base: Int
                    )

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Functional Spark Project")
      .master("local[*]")
      .getOrCreate()

    tryLoadData(spark, "games.csv") match {
      case SuccessResult(df) =>
        val withStats = compose(withRatingStats, withGameOutcome, withTimeStats, selectNumericColumns)
        val statsDF = df
          .transform(withStats)
          .transform(Utils.filterValidRows)

        val assembledDF = assembleFeatures(statsDF)
        val corrMatrix = computeCorrelationMatrix(assembledDF)
        saveCorrelationHeatmap(corrMatrix, statsDF.columns)
        buildScatterPlots(statsDF)

        CalculateAverageTurns.tryCalculateAverageTurns(df) match {
          case SuccessResult(avg) =>
            println(f"Tail Recursive Average Turns: $avg%.2f")
            saveAverageToFile(avg, "average_turns.txt")
            println("Average turns saved to file: average_turns.txt")
          case ErrorResult(msg) =>
            println(s"[Custom Error] Could not compute average: $msg")
        }

      case ErrorResult(msg) =>
        println(s"[Custom Error] Could not load data: $msg")
    }

    scala.io.StdIn.readLine()
  }
}