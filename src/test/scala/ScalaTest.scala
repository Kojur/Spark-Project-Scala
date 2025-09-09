import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import Analysis._
import Utils._
import Combinator._
import CalculateAverageTurns._

class ProjectUnitTests extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("UnitTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sampleDF: DataFrame = Seq(
    ("white", 30, 1500, 1400, true, 0L, 200000L, "5+3", 12),   // 200 seconds
    ("black", 25, 1600, 1600, false, 100L, 600100L, "3+2", 10) // 500 seconds
  ).toDF("winner", "turns", "white_rating", "black_rating", "rated", "created_at", "last_move_at", "increment_code", "opening_ply")


  test("withRatingStats should calculate rating_difference and avg_rating") {
    val result = withRatingStats(sampleDF)
    val row = result.select("rating_difference", "avg_rating", "is_rated").head()
    assert(row.getAs[Int]("rating_difference") == 100)
    assert(row.getAs[Double]("avg_rating") == 1450.0)
    assert(row.getAs[Int]("is_rated") == 1)
  }

  test("withGameOutcome should set white_win correctly") {
    val result = withGameOutcome(sampleDF)
    val outcome = result.select("white_win").as[Int].collect()
    assert(outcome.sameElements(Array(1, 0)))
  }

  test("withTimeStats should compute duration_seconds and increment_base") {
    val result = withTimeStats(sampleDF)
    val rows = result.select("duration_seconds", "increment_base").as[(Double, Int)].collect()
    assert(rows(0)._1 == 200.0)
    assert(rows(0)._2 == 5)
    assert(rows(1)._1 == 600.0)
    assert(rows(1)._2 == 3)
  }

  test("avgTailRec should compute the correct average") {
    val input = List(10, 20, 30)
    val avg = avgTailRec(input)
    assert(avg == 20.0)
  }

  test("tryCalculateAverageTurns should return correct average") {
    val avgResult = tryCalculateAverageTurns(sampleDF)
    assert(avgResult.isInstanceOf[SuccessResult[Double]])
    assert(math.abs(avgResult.asInstanceOf[SuccessResult[Double]].value - 27.5) < 1e-6)
  }

  test("selectNumericColumns should include expected columns") {
    val dfWithStats = compose(withRatingStats, withGameOutcome, withTimeStats)(sampleDF)
    val selected = selectNumericColumns(dfWithStats)
    assert(selected.columns.toSet == Set(
      "turns", "white_rating", "black_rating", "opening_ply",
      "rating_difference", "avg_rating", "white_win", "is_rated",
      "duration_seconds", "increment_base"
    ))
  }

  test("filterValidRows should remove nulls") {
    val dfWithNull = sampleDF.withColumn("turns", when($"turns" === 25, lit(null)).otherwise($"turns"))
    val filtered = filterValidRows(dfWithNull)
    assert(filtered.count() == 1)
  }
}