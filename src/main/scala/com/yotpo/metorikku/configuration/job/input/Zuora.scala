package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.zuora.ZuoraInput

case class Zuora(email: Option[String],
                 password: Option[String],
                 instanceURL: Option[String],
                 zoql: Option[String],
                 pageSize: Option[Int]
               ) extends InputConfig {
  require(email.isDefined, "Zuora input: email is mandatory")
  require(password.isDefined, "Zuora input: password is mandatory")
  require(instanceURL.isDefined, "Zuora input: instanceURL is mandatory")
  require(zoql.isDefined, "Zuora input: zoql is mandatory")

  override def getReader(name: String): Reader = ZuoraInput(
    name=name, email=email.get, password=password.get, instanceURL=instanceURL.get, zoql=zoql.get, pageSize=pageSize)
}
