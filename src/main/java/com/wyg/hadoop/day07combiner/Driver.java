package com.wyg.hadoop.day07combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DESCRIPTION:
 * 驱动类
 *
 * @author wyg0405@gmail.com
 * @create 2019-01-24 23:06
 **/

public class Driver {
  /**
   * @param args
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    //加载配置文件
    Configuration conf = new Configuration();
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    //使 NameNode 返回 DataNode 的主机名而不是 IP
    conf.set("dfs.client.use.datanode.hostname", "true");

    //启动一个Job
    Job job = Job.getInstance(conf);

    //计算程序主驱动类
    job.setJarByClass(Driver.class);

    //设置Mapper及其key,value类型
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //设置reducer及其key,value类型
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    //设置reducetask任务并行度，reducetask任务个数，最终结果为3个
    //job.setNumReduceTasks(3);

    //设置Combiner
    job.setCombinerClass(MyCombiner.class);
    //或者直接用Reducer
    //job.setCombinerClass(WordCountReducer.class);

    //输入路径
    FileInputFormat.addInputPath(job, new Path("hdfs://hadoop1:9000/input/test"));//或者  FileInputFormat.setInputPaths(job,new Path(args[0]));
    //输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/output/wordcount15"));

    //启动job
    //job.submit();//无日志
    job.waitForCompletion(true);
  }

}
