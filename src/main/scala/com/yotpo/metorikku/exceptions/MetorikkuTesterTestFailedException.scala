package com.yotpo.metorikku.exceptions

case class MetorikkuTesterTestFailedException(private val message: String = "",
                                               private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
