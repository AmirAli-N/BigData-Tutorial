#configure jupyterhub to use cypress
	#in jupyter notebook, open a new terminal
		nano .jhubrc
			module add hdp
			#save -> y
			#write to .jhubrc
		#Click Control Panel
		#Stop my server
		#Start my server
	#Spawn a computing server with 1 node, 8 cores, 6gb per core, and 2:00:00 walltime

#access hadoop file system
	#new terminal
		module add hdp
		hdfs dfs -ls #show your hadoop home list directory
			hdfs dfs -ls / #show directories from root directory
			hdfs dfs -ls/user/snasrol #show directories specific to a user
		hdfs dfs -put /user/snasrol/research/sex.csv /user/snasrol/teaching #copy file from research/sex to teaching folder
		hdfs dfs -copyFromLocal /scratch2/snasrol/research/sex.csv /user/snasrol/teaching #copy from local file system to user file system
		hdfs dfs -copyToLocal /user/snasrol/teachhing/sex.core /scratch2/snasrol/research/sex #copy from user file system to local file system
		hdfs dfs -rm -r intro-to-spark #removes the intro-to-spark directory recursively
		hdfs dfs -mkdir intro-to-spark #creates a new directory named intro-to-spark
		hdfs dfs -cat /repository/gutenberg-shakespeare.txt \
			2>/dev/null | head -n 20 #concatenates source path to stdout, 2>/dev/null ignores the error output, | head -n 20 displays the first 20 lines
		