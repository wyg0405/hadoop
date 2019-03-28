package com.wyg.hbase;

import com.wyg.hadoop.day07combiner.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 从hbase读取数据到mapredcue任务
 *
 * @author wyg0405 @gmail.com
 * @create 2019 -03-13 21:15
 */
public class ReadHbaseToHDFS {

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws IOException            the io exception
   * @throws ClassNotFoundException the class not found exception
   * @throws InterruptedException   the interrupted exception
   */
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    //job访问的文件系统
    conf.set("fs.defaultFS", "hdfs://master");

    //hbase的zookeeper地址
    conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4");

    //MapReduce任务提交到集群运行
    /*conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.hostname", "hadoop1");
    conf.set("mapreduce.job.jar", "E:\\project\\hadoop\\out\\artifacts\\hadoop_jar\\hadoop.jar");
    conf.set("mapreduce.app-submission.cross-platform", "true");//意思是跨平台提交，在windows下如果没有这句代码会报错*/


    System.setProperty("HADOOP_USER_NAME", "hadoop");
    conf.set("dfs.client.use.datanode.hostname", "true"); //使 NameNode 返回 DataNode 的主机名而不是 IP

    Job job = Job.getInstance(conf);
    job.setJarByClass(ReadHbaseToHDFS.class);
    job.setJobName("HbaseReadToHDFS");
    Scan scan = new Scan();
    TableMapReduceUtil.initTableMapperJob("stu1".getBytes(), scan,
            MyMapper.class, Text.class, IntWritable.class, job);
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //输出路径,该路径不能存在
    FileOutputFormat.setOutputPath(job, new Path("hdfs://master/hbase_hdfs/21"));

    //启动job
    //job.submit();//无日志
    job.waitForCompletion(true);
  }


  /**
   * The type My mapper.
   */
  public static class MyMapper extends TableMapper<Text, IntWritable> {
    /**
     * The K.
     */
    Text k = new Text();

    /**
     * Map.
     *
     * @param key     the key
     * @param value   the value
     * @param context the context
     * @throws IOException          the io exception
     * @throws InterruptedException the interrupted exception
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      String[] record = value.toString().split("");
      System.out.println(key + "\t" + value);
      for (String s : record) {
        k.set(s);
        context.write(k, new IntWritable(1));
      }
    }
  }


  /**
   * The type My reducer.
   */
  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * Reduce.
     *
     * @param key     the key
     * @param values  the values
     * @param context the context
     * @throws IOException          the io exception
     * @throws InterruptedException the interrupted exception
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable t : values) {
        count += t.get();
      }
      context.write(key, new IntWritable(count));
    }
  }
}
