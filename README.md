# VarDictSpark
to run:
```
spark-submit \
--master yarn-client \
--num-executors 50 \
--conf spark.executor.memory=10g \
--driver-memory=2g \
--class md.fusionworks.vardictspark.VarDictSpark \
target/scala-2.10/VarDictSpark-assembly-0.1.jar \
-c 1 -S 2 -E 3 -s 2 -e 3 -g 4 \
-G /home/ubuntu/work/data/chr20_dataset/raw/human_b37_20.fasta \
-b /home/ubuntu/work/data/chr20_dataset/raw/dedupped_20.bam \
/home/ubuntu/work/data/chr20_dataset/raw/dedupped_20.bed
```