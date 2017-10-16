package com.yotpo.metorikku

import com.yotpo.metorikku.metrics.MetricRunner

// This class is used to allow metric runner in IDEs (loading all provided in compile time)
object Metorikku {
  def main(args: Array[String]): Unit = {
    MetricRunner.main(args)
  }
}
