package com.wyg.hadoop.day05course;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DESCRIPTION:
 * 驱动类
 * 统计成绩，每科最高分，最低分，平均分
 *
 * @author wyg0405@gmail.com
 * @create 2019-01-24 23:06
 **/

public class Driver {
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
    job.setMapperClass(CourseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CourseEntity.class);

    //设置reducer及其key,value类型
    job.setReducerClass(CourseReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CourseEntity.class);

    //设置reducetask任务并行度，reducetask任务个数，最终结果为2个
    //job.setNumReduceTasks(1);
    //自定义Partition类
    //job.setPartitionerClass(MyPartitioner.class);

    //输入路径
    FileInputFormat.addInputPath(job, new Path("hdfs://hadoop1:9000/input/course"));//或者  FileInputFormat.setInputPaths(job,new Path(args[0]));
    //输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/output/course7"));

    //启动job
    //job.submit();//无日志
    job.waitForCompletion(true);
  }

}
