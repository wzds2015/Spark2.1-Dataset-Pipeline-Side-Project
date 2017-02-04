package com.nyu.usract

/**
  * Created by Wenliang Zhao on 1/31/17.
  */

import java.nio.file.Paths
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object UserActivityPipeline extends App {
  import PipelineUtilities._

  main(args)
  override def main(args: Array[String]) : Unit = {
    // spark initialization
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName("User Activity and Segments")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    // spark settings
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions", 60)
    spark.conf.set("spark.driver.memory", "2g")
    spark.conf.set("spark.executor.memory", "2g")
    spark.sparkContext.setLogLevel("WARN")

    val inputFile = args(0)
    val filterFile = args(1)
    val sparseFlag = args(2)
    val inputDs: Dataset[OriginalData] = spark.read.format("com.databricks.spark.csv")
                       .option("delimiter", "\t")
                       .load(inputFile)
                       .toDF("timestamp", "user_id", "segmentString")
                       .as[OriginalData]

    val wallOffMap = readWallOffSegments(filterFile)
    val outputDs = sparseFlag match {
      case "true" => pipeline(inputDs, wallOffMap, spark)
      case _ => pipeline(inputDs, wallOffMap, spark).map(out =>
          sparseToDense(out))
    }

    //outputDs.show(100)
    val outputFile = Paths.get(".", "data", "output_file").toString
    sparseFlag match {
      case "true" => outputDs.asInstanceOf[Dataset[Output]].toDF.write.parquet(outputFile)
      case _ => outputDs.asInstanceOf[Dataset[OutputDense]].toDF.write.parquet(outputFile)
    }

    // stop spark context
    spark.stop()
  }

  /**
    *
    * @param input      input dataset with original data format
    * @param wallOff    wall off segment set
    * @return           dataset with output format (2 options: dense (commented), sparse)
    */
    def pipeline(input: Dataset[OriginalData], wallOff: Set[String], spark: SparkSession) : Dataset[Output] = {
      val filterFlatDs = inputTobucket(parseInput(input, spark), wallOff, spark)
      groupIdMergeToSparseBucket(filterFlatDs, spark)
    }

  /**
    * Step 1
    * @param ds  dataset with original data format
    * @return    split segments string by comma, and group whole row as UserActivity class
    */
  def parseInput(ds: Dataset[OriginalData], spark: SparkSession) : Dataset[UserActivity] = {
    import spark.implicits._
    ds.map(od => UserActivity(od.timestamp, od.user_id, od.segmentString.split(",")))
  }

  /**
    * Step 2
    * @param ds         output of Step 1, dataset with UserActivity format
    * @param wallOff    wall off segment set
    * @return           dataset with all segments exploded
    */
  def inputTobucket(ds: Dataset[UserActivity], wallOff: Set[String], spark: SparkSession) : Dataset[UserActivityBucketFlat] = {
    import spark.implicits._
    ds.map(ua => UserActivityBucket(tsToBucketId(ua.timestamp), ua.user_id,
          filterArrayBasedOnSet(ua.segments, wallOff) ) )
         .flatMap(uab => uab.segments.map(s => UserActivityBucketFlat(uab.bucket_id, uab.user_id, s)))
  }

  /**
    * Step 3
    * @param ds    output of Step 2, dataset with UserActivityBucketFlat format
    * @return      dataset with output format
    */
  def groupIdMergeToSparseBucket(ds: Dataset[UserActivityBucketFlat], spark: SparkSession) : Dataset[Output] = {
    import spark.implicits._
    val tmpDs = ds.map(uab => (uab.user_id, BucketAndSegment(uab.bucket_id, uab.segment) ) )
      .toDF("userId", "inter1").as[idBucketSegment]
      .groupBy("userId").agg(collect_set(col("inter1")) as "bucketSegmentTupleArray")
    tmpDs.map(r => Output(r.getAs[String](0), seqOfBucketAndSegmentToSparseBucket(r.getAs[Seq[Row]](1) ) ) )
  }
}
