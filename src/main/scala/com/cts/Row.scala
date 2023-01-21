package com.cts

case class Row(n: String, a: Int, c: String) {
  private val name: String = n
  private val age: Int = a
  private val city: String = c

  def fetchValue[A](field: String): A = {
    if (field == "name") {
      return name.asInstanceOf[A]
    }
    if (field == "age") {
      return age.asInstanceOf[A]
    }
    if (field == "city") {
      return city.asInstanceOf[A]
    }

    throw new Exception("In Row, function fetchValue, wrong field")
  }
}

