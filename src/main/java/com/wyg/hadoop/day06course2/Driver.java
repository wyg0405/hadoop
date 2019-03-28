package com.wyg.hadoop.day06course2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DESCRIPTION:
 * 驱动类
 * 统计成绩，每科成绩出现相同的分数，该分数次数，及该分数的同学
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
    job.setMapOutputKeyClass(CourseEntity.class);
    job.setMapOutputValueClass(Student.class);

    //设置reducer及其key,value类型
    job.setReducerClass(CourseReducer.class);
    job.setOutputKeyClass(CourseEntity.class);
    job.setOutputValueClass(Student.class);

    //设置reducetask任务并行度，reducetask任务个数，最终结果为2个
    //job.setNumReduceTasks(1);
    //自定义Partition类
    //job.setPartitionerClass(MyPartitioner.class);

    //输入路径
    FileInputFormat.addInputPath(job, new Path("hdfs://hadoop1:9000/input/day06course2"));//或者  FileInputFormat.setInputPaths(job,new Path(args[0]));
    //输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/output/day06course2/21"));

    //启动job
    //job.submit();//无日志
    job.waitForCompletion(true);
  }

}
