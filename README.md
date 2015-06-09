# GenericJoin

Create the project by running
sbt package

Run project by running
/usr/hdp/2.2.4.4-16/spark/bin/spark-submit --master yarn --class GenericJoin --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:NewSize=1G" --num-executors 35 --executor-cores 4 --executor-memory=6G /home/hsbc/generic/target/scala-2.10/generic-join_2.10-1.0.jar
