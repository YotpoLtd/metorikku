package com.yotpo.metorikku.test

case class TableErrorData(errorType: ErrorType.Value, expectedErrorRowsIndexes: List[Int], actualErrorRowsIndexes: List[Int],
                          expectedMismatchedActualIndexesMap: List[(Int, Int)])
//expectedMismatchedActualIndexesMap => (expectedRowIndexMatchingKeyError, actualRowIndexMatchingKeyError)
