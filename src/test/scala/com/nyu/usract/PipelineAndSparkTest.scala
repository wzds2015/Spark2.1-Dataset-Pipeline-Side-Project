package com.nyu.usract

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.scalatest.FunSuite

/**
  * Created by Wenliang Zhao on 2/1/17.
  */
class PipelineAndSparkTest extends FunSuite with DataFrameSuiteBase {
  import UserActivityPipeline._

  // spark-testing-base currently still updating sources for spark 2.0+, not yet finished
  // Currently there is no easy way to test dataset
  // Otherwise here should be integration test for spark dataset manipulation
}
