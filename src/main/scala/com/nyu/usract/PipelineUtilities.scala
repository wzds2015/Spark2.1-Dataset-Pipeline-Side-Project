package com.nyu.usract

import org.apache.spark.sql.Row
import org.joda.time._
import scala.io.Source

/**
  * Created by Wenliang Zhao on 1/31/17.
  */


// case class pool
case class OriginalData(timestamp: String, user_id: String, segmentString: String)

case class UserActivity(timestamp: String, user_id: String, segments: Seq[String])

case class UserActivityBucket(bucket_id: Int, user_id: String, segments: Seq[String])

case class UserActivityBucketFlat(bucket_id: Int, user_id: String, segment: String)

case class TsAndSegments(timestamp: Int, segments: Seq[String])

case class BucketAndSegment(bucket_id: Int, segment: String)

case class idBucketSegment(userId: String, inter1: BucketAndSegment)

case class SparseBuckets(indices: Seq[Int], values: Seq[Seq[String]])

//sealed trait Output {}
// ToDo: nice to combine below two class into one polymorphic class. Tried some code, had non-serializable issue
case class Output(user_id: String, buckets: SparseBuckets)

case class OutputDense(user_id: String, b0: Seq[String], b1: Seq[String], b2: Seq[String],
                       b3: Seq[String], b4: Seq[String], b5: Seq[String], b6: Seq[String],
                       b7: Seq[String], b8: Seq[String], b9: Seq[String], b10: Seq[String],
                       b11: Seq[String])


object PipelineUtilities {
  // constant for number of buckets
  val MAX_LEN: Int = 12
  val timeInterval = 5

  /**
    *
    * @param ts timestamp in string form
    * @return seconds in a minute time bucket (0-4: 0, 5-9: 1, ... 54-59: 11)
    */
  def tsToBucketId(ts: String) : Int = {
    // multiple 1000, convert to millisecond (joda api is for ms)
    val dt = new DateTime(ts.toLong*1000)
    val sec = dt.secondOfMinute().get()
    dt.secondOfMinute().get() / timeInterval
  }

  /**
    *
    * @param filename input wall off file
    * @return a set storing all wall of segments
    */
  def readWallOffSegments(filename: String) : Set[String] = {
    Source.fromFile(filename).getLines.toSet
  }

  /**
    *
    * @param ar       sequence of string
    * @param wallOff  wall off set
    * @return         same sequenc ebut wall off segments filtered
    */
  def filterArrayBasedOnSet(ar: Seq[String], wallOff: Set[String]) : Seq[String] = {
    ar.filter(s => !(wallOff.contains(s)))
  }

  /**
    *
    * @param seq   a sequence of Row struct, the struct represents the combination of bucket_id and segment
    * @return      sparse representation of all bucket_id and corresponding segments (Seq[bucket_id], Seq[Seq[segment] ])
    */
  def seqOfBucketAndSegmentToSparseBucket(seq: Seq[Row]) : SparseBuckets = {
    val seqCleaned = seq.map(r => (r.getAs[Int](0), r.getAs[String](1) ) )
    val seqOfPairs = seqCleaned.groupBy(_._1)
    // remember to remove duplicates
    val seqOfPairsClean = seqOfPairs.map(p => (p._1, p._2.map(_._2).distinct) )
    bucketsToSB(seqOfPairsClean.toSeq)
  }

  /**
    *
    * @param buckets   sequence of (bucket_id, Seq[segment])
    * @return          same as above
    */
  def bucketsToSB(buckets: Seq[(Int, Seq[String])]) : SparseBuckets = {
    SparseBuckets(buckets.map(_._1), buckets.map(_._2))
  }

  /**
    *
    * @param out   output in sparse representation
    * @return      dense representation
    */
  def sparseToDense(out: Output) : OutputDense = {
    val spBuckets = out.buckets
    val sortedBuckets = spBuckets.indices.zip(spBuckets.values).sortWith((tp1, tp2) => tp1._1 < tp2._1)
    val sparseIndices = sortedBuckets.map(_._1)
    val denseBuckets = {0 until MAX_LEN}.map(n => sparseIndices.contains(n) match {
        case true => sortedBuckets(sparseIndices.indexOf(n))._2
        case _ => Seq()
    } )
    // bad design, should pass repeated arguments based on MAX_LEN,
    // ToDo: figure out how to use repeated arguments for case class definition
    OutputDense(out.user_id, denseBuckets(0), denseBuckets(1), denseBuckets(2), denseBuckets(3),
                denseBuckets(4), denseBuckets(5), denseBuckets(6), denseBuckets(7), denseBuckets(8),
                denseBuckets(9), denseBuckets(10), denseBuckets(11))
  }
}
