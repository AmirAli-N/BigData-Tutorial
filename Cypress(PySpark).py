#In jupyter notebook using ! before keywords makes them shell commands
	!module list #list module like a shell command in terminal
	!cypress-kinit #initialize a cypress key
	!klist #shows validation period for the cypress key
	
#Setting path and environment for spark in cypress cluster
	import sys
	import os

	sys.path.insert(0, '/usr/hdp/current/spark2-client/python')
	sys.path.insert(0, '/usr/hdp/current/spark2-client/python/lib/py4j-0.10.6-src.zip')

	os.environ['SPARK_HOME'] = '/usr/hdp/current/spark2-client/'
	os.environ['SPARK_CONF_DIR'] = '/etc/hadoop/synced_conf/spark2/'
	os.environ['PYSPARK_PYTHON'] = '/software/anaconda3/5.1.0/bin/python'

#Spark application configuration setup
	import pyspark
	conf = pyspark.SparkConf() #initiate an object to be of type spark configuraton to set parameter for run a spark application
	conf.setMaster("yarn") #YARN: yet another resource negotiator, setting the resource manager and job scheduler to yet
	conf.set("spark.yarn.queue", "interactive") #this for cypress, runs the job on interactive mode in the yarn queue
	conf.set("spark.driver.memory","4g") #memory of spark driver mode, a resource consumed by wherever spark is running from
	#conf.set("spark.driver.core","1") #number of spark driver cores
	#conf.set("spark.driver.maxResultSize", "1g") #Limit of total size of serialized results of all partitions for each Spark action
	conf.set("spark.executor.instances", "7") #setting a multiplier for executer instances, executers run on the worker nodes
	conf.set("spark.executor.memory","30g") #in this case, 7*30 gigs is reserved for application executer memory
	conf.set("spark.executor.cores","5")
	##when running a spark application, a driver program is initiated, the spark context is initiated in the driver to set JVMs which are run be executers on 
	##working nodes on Hadoop
	sc = pyspark.SparkContext(conf=conf) #initiates the spark context with a configuation object

#Read a text file from hdfs
	textFile = sc.textFile("/repository/gutenberg-shakespeare.txt")
	textFile = sc.textFile("", minPartitions=5, use_unicode=True) #divides it to at least 5 partitions, and returns it as RDD of strings
	textFile.getStorageLevel() #returns RDD's storage level, e.g., (FALSE, FALSE, FALSE, FALSE, 1) by answering the following questions:
		#Does RDD use disk?
		#Does RDD use memory?
		#Does RDD use off-heap memory?
		#Should an RDD be serialized (while persisting)?
		#How many replicas (default: 1) to use (can only be less than 40)?
	textFile.getNumPartitions() #returns the number of partitions

#RDD's recompute every time an action or transgformation are run. To persist RDD's on memory .cache and .persist may be used.
	textFile.cache() #pins the textFile into memory only
	textFile.persist(StorageLevel) #pins the textFile into storage, i.e., memory, disk, or off-heap memory
	textFile.unpersist() #retires the RDD from memory
	
	textFile.count() #count the number of words in the textFile
#Queuing up some transformations functions, these transformations are lazy and will not run untill an action is called
	wordcount = textFile.flatMap(lambda line: line.split(" ")) \#flatmap(f, preservesPartitioning=False), applies function 'f' to RDD, returns the flattened result as RDD
            .map(lambda word: (word, 1)) \#map(f, preservesPartitioning=False), returns a new RDD by applying functon 'f' to each element
            .reduceByKey(lambda a, b: a + b)#reduceByKey(f, numPartitions) merges the data on each mapper and then reduces the data by key and associative function 'f'. 'a' is the accumulated sum in each partition shuffling and 'b' is the current value for the same 'key'.

#Anonymous functions
	lambda <args>: <expr> #this is a construct that works like a function but it is not defined with a name
	g=lambda x: x**2					
	print (g(2))
	#which is equivalent to
	def g(x):
		return x**2
	print(g(2))

#Call an action that initiates the transformations
	wordcount.saveAsTextFile("intro-to-spark/output-wordcount-01") #saves the RDD as a text file
	wordcount.take(20) #show the first 20 elements of the RDD

#challenge: remove punctuation, lowercase, and remove spaces
##this is the step by step approach
	wordcount_challenge_step01=textFile.flatMap(lambda line: line.split(" ")) #return a RDD with strings separated by a space
	#in the returned RDD, lots of '', i.e., empty strings, are counted as strings separated by a space
	wordcount_challenge_step02=wordcount_challenge_step01.filter(lambda word: [word] if word!='' else []) #filters the RDD for elements equal to empty strip
	def rm_punc_case(s_word): #define a custom function for later map transformations
		s_word=s_word.translate(str.maketrans("", "", string.punctuation)) #translate function return a string in which all characters replaces using a table
		#maketrans("", "", string.punctuation) maps every punctuation to none
		s_word=s_word.lower() #change the case of every letter to lower case
		return (s_word, 1) #wraps the lower cased string into a tuple
	wordcount_challenge_step03=wordcount_challenge_step02.map(rm_punc_case) #return a RDD after applying the rm_punc_case_space function
	wordcount_challenge_step04=wordcount_challenge_step03.reduceByKey(lambda accum, n:accum+n) #reduce the RDD by key, accum is the current sum, and n is the current value (which is always 1)
	#tests show there are still 324 empty strings, i.e., '', in the RDD.
	wordcount_cleaned_emptyspace=wordcount_challenge_step04.filter(lambda value: value if value[0]!='' else []) #filters the RDD, value is now a tuple with two elements, so we check if the first element is an empty string
	wordcount_emptyspace=wordcount_cleaned_emptyspace.filter(lambda value: value[0]=='') #check to seee if there are any empty strings left
	wordcount_emptyspace.take(1) #returns a tuple of empty string and its count

#challenge: find the highest average rated movies and its genre
	ratings = sc.textFile("/repository/movielens/ratings.csv")
	ratings.cache()
	ratingHeader = ratings.first() #extract the first row, header
	ratingsOnly = ratings.filter(lambda x: x != ratingHeader)
	movieRatings=ratingsOnly.map(lambda line: line.split(",")) #RDD of lists[userId, movieId, rating, timeStamp]
	movieRatings=movieRatings.map(lambda lst: (lst[1], float(lst[2]))) #RDD of tuples(movieId, rating), changing the second element to a float
	movieRatings_sum=movieRatings.reduceByKey(lambda accum, n: accum+n) #RDD of tuples(movieId, ratings sum)
	movieCount=movieRatings.countByKey() #countByKey returns a dictionary of keys and number of replications
	movieRatings_avg=movieRatings_sum.map(lambda value: (value[0], value[1]/movieCount[value[0]]))#divides the sum stored in the second element of the tuple by the count of movieCount. value[0] identifies the key, i.e., movieId
	movieRatings_sorted=movieRatings_avg.sortBy(lambda value: value[1], ascending=False) #sorts the RDD by value of the average rating stored in the second element of the tuple. It is sorted from largest to smallest
	genres=sc.textFile("/repository/movielens/movies.csv")
	genres=genres.map(lambda line: line.split(","))
	movie_genre=genres.join(movieRatings_sorted) #joins the two RDD by key
	##there is a problem here. Observe that some movie titles in the genres RDD have commas in their record. Therefore, when joined, some part of the movie title after comma is mistaken for the genre of the movie
#there is another way to calculate the average rating
	ratings = sc.textFile("/repository/movielens/ratings.csv")
	ratings.cache()
	ratingHeader = ratings.first() #extract the first row, header
	ratingsOnly = ratings.filter(lambda x: x != ratingHeader)
	movieRatings=ratingsOnly.map(lambda line: line.split(",")) #RDD of lists[userId, movieId, rating, timeStamp]
	movieRatings=movieRatings.map(lambda lst: (lst[1], float(lst[2]))) #RDD of tuples(movieId, rating), changing the second element to a float
	groupByKeyRatings = movieRatings.groupByKey() #groupByKey returns an RDD where elements are grouped by key as iterable object
		#it can be changed into a list by
		groupByKeyRatings.groupByKey().mapValues(list)
		avgRatings = groupByKeyRatings.mapValues(lambda V: sum(V) / float(len(V)))
#another way to sort the data is to show the data as sorted, after joining the two RDDs
	movie_genre.takeOrdered(10, key = lambda x : -x[1][0]) #10 shows the first 10 element
	#recall that when joined the elements look like ('movieId', (rating, 'genres'))
	#x[1][0] points to the rating in the above structure
	#-x[1][0] descending sort according to x[1][0]
	#x[1][0] ascending sort according to x[1][0]

	