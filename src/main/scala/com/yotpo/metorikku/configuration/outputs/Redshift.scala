package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Redshift(@JsonProperty("jdbcURL") jdbcURL: String,
                    @JsonProperty("tempS3Dir") tempS3Dir: String) {
  require(Option(jdbcURL).isDefined, "Redshift Database arguments: jdbcURL is mandatory.")
  require(Option(tempS3Dir).isDefined, "Redshift Database arguments: tempS3Dir is mandatory.")
}
