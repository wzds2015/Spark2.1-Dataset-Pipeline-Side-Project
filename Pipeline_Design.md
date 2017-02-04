# Spark2.1 Pipeline Design

#### Wenliang Zhao (wz927@nyu.edu), Courant Institute

## Pipeline
1. Data reader:
	I use Databricks [[spark-csv](https://github.com/databricks/spark-csv)] to read tsv data (specify delimiter as 't'). **NOTICE:** to read gzip file, the suffix of input file must be *.csv.gz. I read input data into Spark dataset [[Spark-2.1.0](http://spark.apache.org/releases/spark-release-2-1-0.html) structure with 3 columns - timestamp(String), user_id(String), segments(String); also I read wall off file into a set
	Result: Dataset[OriginalData] & wallOffSet (Set[String])
2. Step 1:
	Parse segments into segment array
	Result: Dataset[UserActivity]
3. Step 2:
	Convert timestamp to second in a minute using [[Joda](https://github.com/JodaOrg/joda-time)], then calculate the corresponding bucket (divided by 5); filter segments based on wallOffSet; explode (flatMap) segment array so that each record has one bucket_id, user_id and segment
	Result: Dataset[UserActivityBucketFlat]
4. Step 3:
	Group same user_id, so that each record will be (user_id, Array[(bucket_id, segment)]; for each record, organize by bucket_id, merge all segments belong to same bucket; convert this record into sparse form, for each user_id, I store an array of bucket_id occurred, and their associated segment array. (I also add one more method to convert this sparse representation to the dense one in order to obtain what is shown in the description)
	Result: Dataset[Output] or Dataset[OutputDense]
5. Writer
	Write the output Dataset onto disk. I use parquet format because it's also gzipped and it has stronger ability to represent the complex structure than csv (if sparse table is used). 
	The partition is chosen to 60 considering 5.5g file size with each partition roughly handling 100M data


## Code Structure

There are 4 files in total, 2 in main directory and 2 in test directory. main derectory include a utilities file and a runner file. utilities file include all functions manipulating records in spark Dataset, and a couple of case classes. runner includes the main object "UserActivityPipeline" with a main function "main" with reader, writer and callings of pipelines. There is a function "pipeline" organizing all sub-pipeline (steps). 

## Testing
I include 2 kinds of testing -- [scalatest](http://www.scalatest.org/) and [spark-testing-base](https://github.com/holdenk/spark-testing-base)

1. scalatest
	scalatest in this project is used for unit test on utilities functions. We use the style "FlatSpec + Matchers". I tested all functions (5 plus one tested in another)
2. spark-testing-base
	This is supposed to be used for integration test with spark Dataset. I had some experience on Dataframe before. However, the library haven't fully (there is some description saying that they support Dataset now, but actually not fully) supported Dataset. When I realize this, time is too limited to roll back everything to DataFrame. Also it is my first time dealing with Dataset. I think it is worth to try this new abtraction as it demonstrated to be advanced than RDD and DataFrame. As this reason, I simply leave the part ToDo for future works!!!

## Run code

But before running, let's confirm some convention:

1. input file suffix must be *.csv.gz (if \*.tsv.gz, change file name to follow the convention) -- I can also do this in code, but this is a minor thing.  

2. test file (the small input and wall off file must be put into data directory in advance), because of the requirement of test codes. The big input file can be any where, but need to be specified in the parameter setting.

There are two ways to run the code:

1. sbt: 
	Use runMain in sbt to run the code. Need to specify 3 parameters for the main function - input_file, wall_off_file, sparse_flag("true" mean do sparse representation, other means do the normal output format)
2. assembly jar
	Use sbt assembly to pack the source code into a jar file, and run it accordingly.
	There is a python code "run.py" which automatically run everything. There are 5 steps in run.py
		
		2.1. Remove results from previous run (data/output_file, metastore_db)
		2.2. Clean files due to previous assembly
		2.3. pack the source code using assembly
		2.4. delete META-INF related unrelated files in jar using zip
		2.5. Run the code

**NOTICE:** User need to change paths and parameters in run.py
