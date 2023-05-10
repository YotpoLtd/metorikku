package com.yotpo.metorikku.exceptions

case class MetorikkuInvalidFileException(
    private val message: String = "",
    private val cause: Throwable = None.orNull
) extends Exception(message, cause)
