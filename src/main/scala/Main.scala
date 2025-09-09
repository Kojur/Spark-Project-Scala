// Functional Data Processing Pipeline
// Rewritten for functional programming project compliance
// Includes: custom combinator pipeline, tail-recursion, error handling, and visualization

import Analysis._
import Utils._
import Combinator._
import org.apache.spark.sql._

/** Custom sealed trait to model computation results */
sealed trait Result[+A]

/** Represents a successful result wrapping a value of type A */
case class SuccessResult[A](value: A) extends Result[A]

/** Represents a failed result carrying an error message */
case class ErrorResult(message: String) extends Result[Nothing]

/** Case class to model enriched game statistics */
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

/** Main entry point for the Spark functional analytics application */
object Main {

  /**
   * The main method initializes the Spark session, reads the CSV,
   * applies transformations, generates plots and calculates average turns
   */
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Functional Spark Project")
      .master("local[*]")
      .getOrCreate()

    // Try to load the CSV dataset into a DataFrame
    tryLoadData(spark, "games.csv") match {
      case SuccessResult(df) =>
        // Compose pipeline using custom combinator
        val withStats = compose(
          withRatingStats,
          withGameOutcome,
          withTimeStats,
          selectNumericColumns
        )

        // Apply pipeline and filter invalid rows
        val statsDF = df
          .transform(withStats)
          .transform(Utils.filterValidRows)

        // Assemble features and compute correlation
        val assembledDF = assembleFeatures(statsDF)
        val corrMatrix = computeCorrelationMatrix(assembledDF)

        // Save visualizations
        saveCorrelationHeatmap(corrMatrix, statsDF.columns)
        buildScatterPlots(statsDF)

        // Tail recursive average calculation
        CalculateAverageTurns.tryCalculateAverageTurns(df) match {
          case SuccessResult(avg) =>
            println(f"Tail Recursive Average Turns: $avg%.2f")
            saveAverageToFile(avg, "average_turns.txt")
            println("Average turns saved to file: average_turns.txt")

          case ErrorResult(msg) =>
            // Custom error reporting
            println(s"[Custom Error] Could not compute average: $msg")
        }

      case ErrorResult(msg) =>
        // Custom error for loading failure
        println(s"[Custom Error] Could not load data: $msg")
    }

    // Pause for inspection (optional)
    scala.io.StdIn.readLine()
  }
}
