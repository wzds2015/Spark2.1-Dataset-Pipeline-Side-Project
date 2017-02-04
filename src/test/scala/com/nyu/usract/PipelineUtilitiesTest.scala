package com.nyu.usract

import java.nio.file.Paths

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by admin on 2/1/17.
  */
class PipelineUtilitiesTest extends FlatSpec with Matchers {
  import PipelineUtilities._

  "tsToBucketId" should "converts string timestamp to bucket id between 0 and 11" in {
    val ts1 = "1476896358"
    val ts2 = "1367334263"
    val ts3 = "1066571103"

    tsToBucketId(ts1) should equal (3)
    tsToBucketId(ts2) should equal (4)
    tsToBucketId(ts3) should equal (0)
  }

  "readWallOffSegments" should "read the wallOff file in and store the segments in a Set" in {
    val testFilename = Paths.get(".", "data", "walled_off_segments.txt").toString
    val wallOffSet = readWallOffSegments(testFilename)

    // order changed, still doesn't matter
    wallOffSet should equal (Set("12345", "231", "40", "1110", "12392"))
  }

  "filterArrayBasedOnSet" should "eliminate elements in the array that also presents in set" in {
    val ar1: Array[String] = Array()
    val ar2 = Array("123", "0", "333")
    val ar3 = Array("77", "826", "999")
    val wallOff = Set("333", "0")

    filterArrayBasedOnSet(ar1, wallOff) should equal (Array())
    filterArrayBasedOnSet(ar2, wallOff) should equal (Array("123"))
    filterArrayBasedOnSet(ar3, wallOff) should equal (ar3)
  }

  "seqOfBucketAndSegmentToSparseBucket" should "parse a sequence of Row (bucket_id, segment), group by " +
    "bucket_id and combine segments for same bucket together, then convert result in sparse form" in {
    val seq1 = Seq(Row(2, "123"), Row(2, "345"), Row(3, "11"), Row(3, "22"), Row(3, "123") )
    val seq2 = Seq(Row(2, "123"), Row(2, "345"), Row(3, "11"), Row(3, "22"), Row(3, "123"), Row(2, "456"), Row(2, "567") )
    val seq3 = Seq(Row(2, "123"), Row(2, "345"), Row(3, "11"), Row(3, "22"), Row(3, "123"), Row(2, "456"), Row(2, "123") )

    seqOfBucketAndSegmentToSparseBucket(seq1) should equal (SparseBuckets(Seq(2, 3), Seq(Seq("123", "345"), Seq("11", "22", "123"))))
    seqOfBucketAndSegmentToSparseBucket(seq2) should equal (SparseBuckets(Seq(2, 3), Seq(Seq("123", "345",
      "456", "567"), Seq("11", "22", "123"))))
    seqOfBucketAndSegmentToSparseBucket(seq3) should equal (SparseBuckets(Seq(2, 3), Seq(Seq("123", "345", "456"), Seq("11", "22", "123"))))
  }

  "sparseToDense" should "convert a sparse output into it's dense form" in {
    val out1 = Output("1234567", SparseBuckets(Seq(2, 3, 8, 10), Seq(Seq("111", "222"), Seq("000"), Seq("111", "000"), Seq("777"))))
    val out2 = Output("1234567", SparseBuckets(Seq(3, 8, 2, 10), Seq(Seq("111", "222"), Seq("000"), Seq("111", "000"), Seq("666"))))


    sparseToDense(out1) should equal (OutputDense("1234567", Seq(), Seq(), Seq("111", "222"), Seq("000"), Seq(), Seq(),
      Seq(), Seq(), Seq("111", "000"), Seq(), Seq("777"), Seq()))
    sparseToDense(out2) should equal (OutputDense("1234567", Seq(), Seq(), Seq("111", "000"), Seq("111", "222"), Seq(), Seq(),
      Seq(), Seq(), Seq("000"), Seq(), Seq("666"), Seq()))
  }
}
