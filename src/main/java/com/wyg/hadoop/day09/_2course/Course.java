package com.wyg.hadoop.day09._2course;

import com.wyg.hadoop.day08Counter.CounterTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DESCRIPTION:
 * 分组排序，按课程分组，并按平均分由高到低排序
 * math	tom	89	98	34	23	56
 * han	tom	78	23	54	52	94	63
 * english	tom	45	36	98	52	74
 * computer	tom	67	23	89	44	86
 * math	lucy	45	25	66	88	44
 * han	lucy	78	78	45	62	87
 * english	lucy	89	84	66	98	21
 * computer	lucy	25	78	41	62	41
 * math	sum	56	89	54	75	12
 * han	sum	29	12	45	78	32
 * english	sum	62	12	21	45	78
 * computer	sum	67	45	78	12	98
 * math	lily	14	87	52	16	45
 * han	lily	67	45	78	12	32
 * english	lily	88	12	45	78	12
 * computer	lily	96	78	45	12	74
 * math	jim	34	45	46	89	12
 * han	jim	67	23	89	54	78
 * english	jim	48	45	78	12	45
 * computer	jim	35	12	45	78	45
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-14 11:09
 **/

public class Course {

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
    job.setMapOutputKeyClass(CourseEntity.class);
    job.setMapOutputValueClass(Text.class);

    //设置reducer及其key,value类型
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(CourseEntity.class);
    job.setOutputValueClass(Text.class);


    //输入路径
    FileInputFormat.addInputPath(job, new Path("/input/course3"));
    //输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("/output/course3/4"));

    //启动job
    //job.submit();//无日志
    job.waitForCompletion(true);
  }

  public static class MyMapper extends Mapper<LongWritable, Text, CourseEntity, Text> {
    Text v = new Text();
    CourseEntity courseEntity = new CourseEntity();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      int count = 0;
      double sum = 0;
      for (int i = 2; i < line.length; i++) {
        sum += Double.parseDouble(line[i]);
        count++;
      }
      courseEntity.setCourse(line[0]);
      courseEntity.setAvg(sum / count);
      v.set(line[1]);
      context.write(courseEntity, v);
    }


  }

  public static class MyReducer extends Reducer<CourseEntity, Text, CourseEntity, Text> {
    Text v = new Text();

    @Override
    protected void reduce(CourseEntity key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text text : values) {
        v.set(text.toString());
        context.write(key, v);
      }
    }
  }
}
