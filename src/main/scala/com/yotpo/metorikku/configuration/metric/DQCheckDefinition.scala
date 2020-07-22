package com.yotpo.metorikku.configuration.metric

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class DQCheckDefinition(@JsonScalaEnumeration(classOf[DQCheckOpTypeReference]) op: DQCheckOpType.Op,
                             column: Option[String],
                             values: List[Option[Any]],
                             level: Option[String]) extends Serializable
case class DQCheckDefinitionList(checks: List[DQCheckDefinition],
                                 level: Option[String]) extends Serializable

object DQCheckOpType extends Enumeration {
  type Op = Value

  val IsComplete,
  IsUnique = Value
}

class DQCheckOpTypeReference extends TypeReference[DQCheckOpType.type]
