import org.scalatest.funsuite.AnyFunSuite
import com.cts.Row
import com.cts.Utils.dropDuplicates

class UtilsTest extends AnyFunSuite {
  test("Utils.dropDuplicates | 1 dup same person/age") {
    val rows: List[Row] = List(
      Row("loic", 35, "durham"),
      Row("tom", 40, "paris"),
      Row("loic", 35, "london")

    )
    val actual = dropDuplicates(rows, Seq(("name", StringContext), ("age", Int)))
    val expected = List(
      Row("loic", 35, "durham"),
      Row("tom", 40, "paris"),
    )
    assert(actual === expected)
  }

  test("Utils.dropDuplicates | 2 dups same person/age, spread randomly") {
    val rows: List[Row] = List(
      Row("loic", 35, "durham"),
      Row("tom", 40, "paris"),
      Row("loic", 35, "london"),
      Row("anna", 35, "berlin"),
      Row("johanna", 35, "madrid"),
      Row("loic", 35, "london")
    )

    val actual = dropDuplicates(rows, Seq(("name", StringContext), ("age", Int)))

    // dups
    val countLoic = actual.count(r => r.n == "loic" && r.a == 35)
    val countJohanna = actual.count(r => r.n == "johanna")

    // distinct
    val countAnna = actual.count(r => r.n == "anna")
    val countTom = actual.count(r => r.n == "tom")

    assert(countLoic === 1)
    assert(countJohanna === 1)
    assert(countAnna === 1)
    assert(countTom === 1)
  }

  test("Utils.dropDuplicates | 4 dups different person/age + 1 same person w. different age, spread randomly") {
    val rows: List[Row] = List(
      Row("loic", 35, "durham"),
      Row("tom", 40, "paris"),
      Row("loic", 35, "london"),
      Row("anna", 35, "berlin"),
      Row("johanna", 35, "madrid"),
      Row("loic", 35, "london"),
      Row("abdul", 88, "newcastle"),
      Row("rohit", 33, "delhi"),
      Row("anna", 35, "bordeaux"),
      Row("anna", 36, "bordeaux"),
    )

    val actual = dropDuplicates(rows, Seq(("name", StringContext), ("age", Int)))

    // dups
    val countLoic = actual.count(r => r.n == "loic" && r.a == 35)
    val countAnna35 = actual.count(r => r.n == "anna" && r.a == 35)

    // distinct
    val countAnna36 = actual.count(r => r.n == "anna" && r.a == 36)
    val countJohanna = actual.count(r => r.n == "johanna")
    val countTom = actual.count(r => r.n == "tom")
    val countAbdul = actual.count(r => r.n == "abdul")
    val countRohit = actual.count(r => r.n == "rohit")

    assert(countLoic === 1)
    assert(countAnna35 === 1)
    assert(countAnna36 === 1)
    assert(countJohanna === 1)
    assert(countTom === 1)
    assert(countAbdul === 1)
    assert(countRohit === 1)
  }
}
