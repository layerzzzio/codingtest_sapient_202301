package com.cts

import com.cts.Main.PATH

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Utils {
  /**
   * Reads a CSV from a given location
   * WARNING It is not generic (but can be improved)
   * Indeed, it assumes the number of columns are known as well as their data types.
   */
  def readCSV(filename: String): List[Row] = {
    val bufferedSource = Source.fromFile(filename)
    val rows: ListBuffer[Row] = ListBuffer()

    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      rows.append(Row(cols(0), cols(1).toInt, cols(2)))
    }

    bufferedSource.close
    rows.toList
  }

  /** Returns a new list of rows with duplicate rows removed, considering only the subset of fields. */
  def dropDuplicates(rows: List[Row],
                     fieldNames: Seq[(String, Object)]): List[Row] = {
    rows.groupBy(x => groupByGroup(x, fieldNames, List())).map(x => x._2.head).toList
  }

  /** Returns a tuple of fields used in dropDuplicates to groupBy. */
  @tailrec
  private def groupByGroup(row: Row, fieldNames: Seq[(String, Object)], group: List[Any]): List[Any] = {
    if (fieldNames.isEmpty) {
      group
    }
    else {
      val field = fieldNames.head
      val sType = field._2
      val sField = field._1
      val value = row.fetchValue[sType.type](sField)
      val newGroup = group ++ List(value)
      val newFieldNames = fieldNames.filter(f => f != field)
      groupByGroup(row, newFieldNames, newGroup)
    }
  }
}


