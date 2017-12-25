package com.yotpo.metorikku.metric

import com.yotpo.metorikku.metric.config.Step

case class MetricConfig(steps: List[Step], output: List[Map[String, Any]])