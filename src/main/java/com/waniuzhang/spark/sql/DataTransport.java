package com.waniuzhang.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by 1 on 2020/3/20.
 */
public class DataTransport {

    public static void main(String[] args){
        //创建sparkSession实例
        SparkSession sparksql = SparkSession.builder().appName("data-transport").enableHiveSupport().getOrCreate();
        //配置相关属性
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","zhangbt3");
        properties.setProperty("driver","com.mysql.jdbc.Driver");


        //读取源表
        Dataset<Row> dataset = sparksql.read().jdbc("jdbc:mysql://localhost:3306/db01", "(select * from dev_log) as tmp", properties);
        //使用dataframe api
        dataset.show();
        //只筛选部分字段
        dataset.select("id","name","creator").show();

        dataset.printSchema();

        //注册临时表
        dataset.createOrReplaceTempView("dev_tmp");
        //过滤数据
        Dataset<Row> result = sparksql.sql("select * from dev_tmp where creator = 'hdfs'");
        //存储到目标表
        result.write().mode("append").format("Hive").saveAsTable("hive_test.dev_log");
        //关闭实例
        sparksql.close();
    }
}
