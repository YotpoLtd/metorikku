package com.yotpo.metorikku.runners

// This class is used to allow metric runner in IDEs (loading all provided in compile time)
object Metorikku {
  def main(args: Array[String]): Unit = {
    com.yotpo.metorikku.Metorikku.main(args)
  }
}
