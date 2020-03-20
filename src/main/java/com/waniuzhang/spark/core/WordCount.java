package com.waniuzhang.spark.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by 1 on 2020/3/20.
 */
public class WordCount {
    public static void main(String[] args){

        //接受输入输出
        String inPath = args[0];
        String outPath = args[1];

        //创建sparkContext实例
        SparkSession sparkSession = SparkSession.builder().appName("wordcount").getOrCreate();

        //转成java实例
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        //读取文件
        JavaPairRDD<String, Integer> result = sparkContext.wholeTextFiles(inPath)
                .flatMap(file -> Arrays.asList(file._2.split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word,1))
                .reduceByKey((a,b) -> a+b);

        List<String> list = new ArrayList<>();
        //对大变量进行广播
        Broadcast<List<String>> broadcast = sparkContext.broadcast(list);
        broadcast.getValue();//获取值
        broadcast.destroy();//销毁

        //写出文件
        result.saveAsTextFile(outPath);
        //关闭实例
        sparkSession.stop();

    }
}
