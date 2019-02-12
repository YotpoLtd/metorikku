package com.yotpo.metorikku.configuration.job.output

case class Redshift(jdbcURL: String,
                    tempS3Dir: String) {
  require(Option(jdbcURL).isDefined, "Redshift Database arguments: jdbcURL is mandatory.")
  require(Option(tempS3Dir).isDefined, "Redshift Database arguments: tempS3Dir is mandatory.")
}
