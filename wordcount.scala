----------------------------------------------------------------------------------------------------------------------
-- Program 	:	Word Count Program in Spark
----------------------------------------------------------------------------------------------------------------------
-- Logic	:	1. Load the file into RDD 		
--				2. Split the each lines into separate words based on the separators in the file ( Using the FlatMap )
--				3. Append one to each word for countiung the occurence ( Use Map )
--				4. Group the words by using reduceByKey
----------------------------------------------------------------------------------------------------------------------
-- Use collect function to test the intermediate stage data
-- line_rdd.collect 					=> Array[String] = Array(dinesh,10000, sathish,2000, mallu,3999, venkat,40000)
-- line_rdd.collect.foreach(println)	=> dinesh,10000
-- 										   sathish,2000
-- 										   mallu,3999
-- 										   venkat,40000
----------------------------------------------------------------------------------------------------------------------
-- Code: Method 1

-- file should be in HDFS/mesos. File is loaded into the RDD
-- At this stage data will be taken as shown below
-- Array[String] = Array(dinesh,10000, sathish,2000, mallu,3999, venkat,40000)
-- dinesh,10000
-- sathish,2000
-- mallu,3999
-- venkat,40000

var line_rdd = sc.textFile("/user/cloudera/input/test")	 
 
-- split the words by separators. In this case we are splitting it based on comma as it is CSV file
-- flatMap is used to flatten the each word into a separate line for further processing
-- If we use map function, then the data will be considered as nested Array further making it difficult to process futher
-- At this stage data will be taken as shown below
-- dinesh
-- 10000
-- sathish
-- 2000
-- mallu
-- 3999
-- venkat
-- 40000
var lines_to_words = line_rdd.flatMap( x => x.split(',')) 

-- Append 1 to each word for counting
-- At this stage data will be taken as shown below
-- (dinesh,1)
-- (10000,1)
-- (sathish,1)
-- (2000,1)
-- (mallu,1)
-- (3999,1)
-- (venkat,1)
-- (40000,1)
var add_one = lines_to_words.map( x => (x,1))

-- Add up one's for all occurence of each word
-- At this stage the data will be taken as shown below
-- (40000,1)
-- (3999,1)
-- (sathish,1)
-- (2000,1)
-- (dinesh,1)
-- (mallu,1)
-- (venkat,1)
-- (10000,1)
var word_cnt = add_one.reduceByKey( (x,y) => (x+y) )

----------------------------------------------------------------------------------------------------------------------------------
-- Method 2

var word_cnt = sc.textFile("/user/cloudera/input/test").flatMap( x => x.split(',')).map( x => (x,1) ).reduceByKey((x,y) => (x+y)) 