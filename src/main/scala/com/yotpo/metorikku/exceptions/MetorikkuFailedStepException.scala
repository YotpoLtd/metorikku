package com.yotpo.metorikku.exceptions

case class MetorikkuFailedStepException(private val message: String = "",
                                        private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
