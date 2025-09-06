import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello! Scala is Working!")
    val spark = SparkSession.builder()
      .appName("Hello Spark!")
      .master("local[*]")
      .getOrCreate()
    println("Spark Session was created!")
    println("Go to http://localhost:4040 to see Spark UI")
    println("Press Enter to exit...")

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("games.csv")


    val winnerDistribution = df.groupBy("winner")
      .count()
      .withColumn("percentage", col("count") * 100 / df.count())

    println("Winner Distribution:")
    winnerDistribution.show()
    // 2. Game Termination Analysis
    val victoryStatus = df.groupBy("victory_status")
      .count()
      .orderBy(desc("count"))

    println("Victory Status Distribution:")
    victoryStatus.show() // :)

    // 3. Most Popular Openings
    val popularOpenings = df.groupBy("opening_name")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    println("Top 10 Popular Openings:")
    popularOpenings.show() //:)

    // 4. Opening Win Rates (for White)
    val openingWinRatesWhite = df.filter(col("winner") === "white")
      .groupBy("opening_name")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count", "white_wins")

    println("Top 10 Openings for White by Wins:")
    openingWinRatesWhite.show(10)

    CorrelationAnalysis.buildCorrelationAnalysis(df);
    val dfForCorr = CorrelationAnalysisScatterPlot.correlationAnalysisScatterPlot(df);
    val columnNames = dfForCorr.columns
    ScatterPlotMatrix.buildScatterPlots(dfForCorr,columnNames);
    scala.io.StdIn.readLine()
  }
}