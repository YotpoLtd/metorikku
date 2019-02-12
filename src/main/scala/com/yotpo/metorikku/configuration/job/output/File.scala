package com.yotpo.metorikku.configuration.job.output


case class File(dir: String,
                checkpointLocation: Option[String]) {
  require(Option(dir).isDefined, "Output file directory: dir is mandatory.")
}
