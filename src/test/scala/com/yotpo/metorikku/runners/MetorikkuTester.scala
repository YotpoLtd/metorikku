package com.yotpo.metorikku.runners

// This class is used to allow metric runner in IDEs (loading all provided in compile time)
object MetorikkuTester {
  def main(args: Array[String]): Unit = {
    com.yotpo.metorikku.MetorikkuTester.main(args)
  }
}
