package com.wyg.hadoop.day08Counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-13 17:24
 **/

public class CounterTest {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    //加载配置文件
    Configuration conf = new Configuration();
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
    //使 NameNode 返回 DataNode 的主机名而不是 IP
    conf.set("dfs.client.use.datanode.hostname", "true");
    //启动一个Job
    Job job = Job.getInstance(conf);

    //计算程序主驱动类
    job.setJarByClass(CounterTest.class);

    //设置Mapper及其key,value类型
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    //设置reducer及其key,value类型
       /* job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/

    //无需reduce过程
    job.setNumReduceTasks(0);

    //输入路径
    FileInputFormat.addInputPath(job, new Path("/input/flow"));
    //输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("/output/flow/flow_counter_6"));

    //启动job
    //job.submit();//无日志
    job.waitForCompletion(true);
  }

  public static class MyMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Counter counterLine = context.getCounter(MyCounter.LINE);
      counterLine.increment(1L);
      String[] words = value.toString().split("\t");
      Counter counterWords = context.getCounter(MyCounter.WORDS);
      counterWords.increment(words.length);

    }
  }

}
