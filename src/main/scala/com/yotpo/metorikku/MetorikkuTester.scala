package com.yotpo.metorikku

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.TesterConfigurationParser.MetorikkuTesterArgs
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.TestUtils
import org.apache.log4j.LogManager
import scopt.OptionParser


object MetorikkuTester extends App {
  lazy val log = LogManager.getLogger(this.getClass)
  val metorikkuTesterArgs = TesterConfigurationParser.parser.parse(args, MetorikkuTesterArgs()).getOrElse(MetorikkuTesterArgs())

  metorikkuTesterArgs.settings.foreach(settings => {
    val metricTestSettings = TestUtils.getTestSettings(settings)
    val config = TestUtils.createMetorikkuConfigFromTestSettings(settings, metricTestSettings, metorikkuTesterArgs.preview)
    Session.init(config)
    TestUtils.runTests(metricTestSettings)
  })

}

object TesterConfigurationParser {
  val NumberOfPreviewLines = 10

  case class MetorikkuTesterArgs(settings: Seq[String] = Seq(), preview: Int = NumberOfPreviewLines)

  val parser: OptionParser[MetorikkuTesterArgs] = new scopt.OptionParser[MetorikkuTesterArgs]("MetorikkuTester") {
    head("MetorikkuTesterRunner", "1.0")
    opt[Seq[String]]('t', "test-settings")
      .valueName("<test-setting1>,<test-setting2>...")
      .action((x, c) => c.copy(settings = x))
      .text("test settings for each metric set")
      .validate(x => {
        if (x.exists(f => !Files.exists(Paths.get(f)))) {
          failure("One of the file is not found")
        }
        else {
          success
        }
      })
      .required()
    opt[Int]('p', "preview").action((x, c) =>
      c.copy(preview = x)).text("number of preview lines for each step")
    help("help") text "use command line arguments to specify the settings for each metric set"
  }
}
