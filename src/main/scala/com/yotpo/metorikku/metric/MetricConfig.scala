package com.yotpo.metorikku.metric

import com.yotpo.metorikku.metric.config.{Output, Step}

case class MetricConfig(steps: List[Step], output: List[Output])