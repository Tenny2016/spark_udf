package com.waniuzhang.spark.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * 自定义UDF
 * Created by 1 on 2020/3/20.
 */
public class NagaUdf {

    public static void main(String[] args){

        //创建实例
        //enableHiveSupport 支持hive连接
        SparkSession spark = SparkSession.builder().appName("naga-udf").enableHiveSupport().getOrCreate();
        //创建自定义udf-字符串长度
        //DataTypes.IntegerType 返回的数据类型
        spark.udf().register("strLen", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        //创建自定义udf-转为json串
        spark.udf().register("toJson", new UDF2<String, String, String>() {
            @Override
            public String call(String k, String v) throws Exception {
                return String.format("{\"%s\":\"%s\"}",k,v);
            }
        },DataTypes.StringType);
        //查询hive
        spark.sql(" select user_name, strLen(user_name) as len, toJson(time,user_name) as json from hive_test.partition_table").show();

        //关闭sparkSession实例
        spark.close();

    }
}
