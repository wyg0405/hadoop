package com.wyg.hadoop.day09._1friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * DESCRIPTION:
 * 求共同好友
 * A:B,C,D,G
 * B:F,G
 * C:A,
 * D D:F
 * E:B,C,F
 * F:C,D,G
 * G:A,F
 * 例如，A关注B,C,D,G；B关注F,G，AB共同好友G
 *
 * <p>最终结果
 * A-B G
 * A-C D
 * A-E C,B
 * A-F C,G,D
 * B-D F
 * B-E F
 * B-F G
 * B-G F
 * C-F D
 * C-G A
 * D-E F
 * D-G F
 * E-F C
 * E-G F
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-13 20:12
 */
public class Friends {
  public static void main(final String[] args)
          throws IOException, ClassNotFoundException, InterruptedException {
    // 加载配置文件
    final Configuration conf = new Configuration();
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
    // 使 NameNode 返回 DataNode 的主机名而不是 IP
    conf.set("dfs.client.use.datanode.hostname", "true");
    // 启动一个Job
    final Job job = Job.getInstance(conf);

    // 计算程序主驱动类
    job.setJarByClass(Friends.class);

    // 设置Mapper及其key,value类型
    job.setMapperClass(MyMapper1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // 设置reducer及其key,value类型
    job.setReducerClass(MyReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);
    // 输入路径
    FileInputFormat.addInputPath(job, new Path("/input/friends"));
    // 输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("/output/friends/4"));

    final Job job2 = Job.getInstance(conf);

    // 计算程序主驱动类
    job2.setJarByClass(Friends.class);

    // 设置Mapper及其key,value类型
    job2.setMapperClass(MyMapper2.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    // 设置reducer及其key,value类型
    job2.setReducerClass(MyReducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    // 输入路径
    FileInputFormat.addInputPath(job2, new Path("/output/friends/4"));
    // 输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job2, new Path("/output/friends/4/1"));

    // 启动job
    // job.submit();//无日志
    // job.waitForCompletion(true);

    // job串联
    final JobControl jc = new JobControl("friend");
    // 将原生job转换为可控Job
    final ControlledJob controlledJob1 = new ControlledJob(job.getConfiguration());
    final ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
    // 添加依赖关系
    controlledJob2.addDependingJob(controlledJob1);

    // 添加到组
    jc.addJob(controlledJob1);
    jc.addJob(controlledJob2);
    // 启动,ControlledJob实现Runnable接口
    final Thread thread = new Thread(jc);
    thread.start();

    // job完成，线程不会自动停止
    while (!jc.allFinished()) {
      Thread.sleep(300);
    }
    thread.stop();
  }

  public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {
      final String[] user_friend = value.toString().split(":");
      final String[] friends = user_friend[1].split(",");
      for (final String friend : friends) {
        k.set(friend);
        v.set(user_friend[0]);
        context.write(k, v);
      }
    }
  }

  public static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
      final StringBuffer stringBuffer = new StringBuffer();
      for (final Text v : values) {
        stringBuffer.append(",").append(v.toString());
      }
      v.set(stringBuffer.substring(1));
      context.write(key, v);
    }
  }

  public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {
      final String[] lines = value.toString().split("\t");
      final String[] users = lines[1].split(",");
      for (final String user : users) {
        for (final String userNext : users) {
          if (user.compareTo(userNext) < 0) {
            k.set(user + "-" + userNext);
            v.set(lines[0]);
            context.write(k, v);
          }
        }
      }
    }
  }

  public static class MyReducer2 extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
      final StringBuffer sb = new StringBuffer();
      for (final Text t : values) {
        sb.append(",").append(t.toString());
      }
      v.set(sb.substring(1));
      context.write(key, v);
    }
  }
}
