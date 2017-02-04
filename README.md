# Spark2.1-Dataset-Pipeline 

## Overview

Your mission, should you choose to accept it, is to take the provided data file containing information about user activity and segment membership, and transform it into
a collection of activity by segment, user, and time slot represented as one of a number
of buckets based upon the minute within the hour that events occurred.

As an added wrinkle, at the last minute, your new boss, Tronald Drump, has decided that there are certain groups of undesirable segments that he wants rejected from the data.  An additional file containing these segment IDs can be found in the  [walled_off_segments.txt](walled_off_segments.txt) file within this repository.  Any segment IDs present in this dataset[*](#Notes) should be effectively "walled off" from making their way into further steps of the data pipeline.

Further details on the input data, desired outputs, and further requirements and constraints
can be found in the following sections.

## Requirements

You may use whatever language, tools, and additional libraries that you see fit,
as long as the following basic requirements are fulfilled:

  1. You must use [Apache Spark](http://spark.apache.org), version 2.0+ as part of your solution.
  1. Commit any changes to a new branch, push, and create a pull request in BitBucket when you are ready to start showing your work.
  1. Make sure that there are sufficient and easy-to-find instructions and documentation contained within the repository so that we can build, run, and test what you've done as appropriate.

## What we'll be looking for

This is intentionally a very open ended exercise, so there is no single right or wrong answer.

We will be looking for things like:

1. How you approached and thought through the problem
1. How well you know and used the features of the programming languages, tools, and libraries you made use of
1. How professionally "put together" and engineered everything is (is the code clean? Are the tests comprehensive? etc)

Remember, though, this is a chance for you to be creative and show off, so let us see what you can do, and try to have some fun with it!

## Input Data

The sections below outline the content and format of the provided input data for you to use during this exercise.

Keep in mind that as is often the case, the input data as the methods and format of outputs suggested might be imperfect or less than optimal.  Don't hesitate to include any suggestions you have on how they could be handled better!

### User activity and segments

The provided input data file (available [here](http:temp) : 5.5GB) is a tab-separated file, with each row containing a single record with the following schema:

|Column Name|Type|Description|
|-----------|----|-----------|
| timestamp | Int | Timestamp, in UNIX epoch time |
| user_id | String | anonymized user id |
| segments | String | Comma-separated list of integer segment IDs |

There is a smaller 1000-line sample of the larger file within this repository for quick use in prototyping and testing.

#### Sample

The following sample lines show two individual rows from the input file representing activity from  two different users at 2016-10-19 12:59:18-0400 and 2016-10-19 13:05:39-0400, respectively, and each belonging to several segment IDs.

Note that the same user IDs can show up at many different times, with potentially different segment memberships.

```
1476896358	430f89a0-9d5c-465e-a3d7-73b6fd2b459b	3460,3461,3462,3463,3464,3465,24585
1476896739	9ec070a0-d328-4136-8786-928611fc5461	12420,12405,23167,343,2589484,15055,12688,12346,12110,15049,12126
```
### "Walled Off" Segment IDs

The walled off segment IDs input data file can be found in the walled_off_segments.txt] file within this repository.

It contains only one column:

|Column Name|Type|Description|
|-----------|----|-----------|
| segment_id | Int | Segment ID |

## Outputs

You should produce and save (via any format or method you choose, as long as all of the previously mentioned constraints and requirements are met) a new dataset containing all segment IDs present during any events in the input dataset (minus those that were walled off) bucketed by the minute within the hour that events occurred ([see below](#Buckets)), grouped by user ID.

For example, using the sample data, [above](#Sample), and assuming that segment ID 3460 is walled off: we would expect the output dataset to be:

|user_id|0-4|5-9|10-14|15-19|20-24|25-29|30-34|35-39|40-44|45-49|50-54|55-59|
|-------|---|---|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
|430f89a0-9d5c-465e-a3d7-73b6fd2b459b||||||||||||3461,3462,3463,3464,3465,24585|
|9ec070a0-d328-4136-8786-928611fc5461||12420,12405,23167,343,2589484,15055,12688,12346,12110,15049,12126||||||||||||

In addition to saving these data, output any other additional summary information that might be useful to the user when run from the console (for example, the number of times walled off segments were seen).

### Buckets

Use the following ranges to separate your results into the appropriate minute-within-the-hour buckets (all ranges inclusive):

1. 0-4
1. 5-9
1. 10-14
1. 15-19
1. 20-24
1. 25-29
1. 30-34
1. 35-39
1. 40-44
1. 45-49
1. 50-54
1. 55-59
