package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.test.ConfigurationParser
import com.yotpo.metorikku.test.Tester
import org.apache.log4j.LogManager

object MetorikkuTester extends App {
  lazy val log = LogManager.getLogger(this.getClass)
  val configs = ConfigurationParser.parse(args)

  configs.foreach(config => {
    Tester(config).run
  })

}


