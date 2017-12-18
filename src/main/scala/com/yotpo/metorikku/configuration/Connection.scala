package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty

case class Connection(@JsonProperty("db_address") dbAddress: String,
                      @JsonProperty("db_port") dbPort: String,
                      @JsonProperty("db_name") dbName: String,
                      @JsonProperty("db_driver") dbDriver: String,
                      @JsonProperty("username") username: String,
                      @JsonProperty("password") password: String) {}
