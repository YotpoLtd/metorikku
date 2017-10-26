package com.yotpo.metorikku.exceptions


case class MetorikkuException(private val message: String = "",
                              private val cause: Throwable = None.orNull)
  extends Exception(message, cause)