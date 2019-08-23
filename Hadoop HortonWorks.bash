#hadoop file system via horton works sandbox
#the commands start with hadoop fs instead of hdfs dfs
	ls -l #uses a long listed format to report
	ls -a #shows all including hidden files
	ls -la #combines both
	hadoop fs -rmdir ml-100k #removes the ml-100k directory form hadoop file system
	
	
	su root #Unix command allowing to run other commands with the previlidge of another user.
	python RatingsBreakdown.py u.data #runs RatingsBreakdown script with u.data file locally, but uses mapreduce logic in mapping and reducing
	python RatingsBreakdown.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data #runs the script on a hadoop cluster, -r hadoop tells MRJob that hadoop is used to run the job, --hadoop-streaming-jar /usr... secifies the java machine python streaming (This is specific to HortonWorks sandbox), u.data is the input file. In an acutal scenario, path of the data file in hadoop cluster should be specified.
	
	
	