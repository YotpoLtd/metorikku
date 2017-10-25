package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Redshift(@JsonProperty("jdbcURL") jdbcURL: String,
                    @JsonProperty("tempS3Dir") tempS3Dir: String){}

object Redshift {
  def apply(): Redshift = new Redshift("" , "/")
}