confNum=0
while read params;

do
confNum=$((confNum+1));
for i in {1..3};
do
java $params -jar top.jar hdfs:///training/hadoop/hw2/input hdfs:///training/hadoop/hw2/output/config$confNum/bid_result$i.txt 10

done

done <jvmconfigs.txt
