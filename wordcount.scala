var test_rdd = sc.textFile("/user/cloudera/input/test")
var split_to_words = test_rdd.flatMap(x => x.split(","))
var add_one_to_words = split_to_words.map(x => (x,1))
var count_words = add_one_to_words.reduceByKey((x,y) => (x+y))
count_words.collect
