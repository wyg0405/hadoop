package com.wyg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 从hdfs写数据到hbase
 * student.txt
 * 001  tom 12  male  MA
 * 002  lily  13  female  CS
 * 003  lucy  13  female  CS
 * 004  jim 14  male  MA
 * 005  kate  12  female  FN
 * 006  tom 12  male  CH
 * 007  david 14  male  FN
 * 008  lilei 13  male  CH
 * 009  hanmeimei 14  female  EN
 * 010  jack  11  male  EN
 * 011  rose  12  female  PH
 * 012  kobe  12  male  PH
 * 013  james 14  male  MA
 * 014  annie 12  female  CH
 * 015  jin 12  male  CS
 *
 * @author wyg0405 @gmail.com
 * @create 2019 -03-14 15:00
 */
public class ReadHdfsToHbase {

  /**
   * Method description
   *
   * @param args the input arguments
   * @throws IOException            the io exception
   * @throws ClassNotFoundException the class not found exception
   * @throws InterruptedException   the interrupted exception
   * @author
   * @date 2019.03.20 22:44:04
   */
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    // job访问的文件系统
    conf.set("fs.defaultFS", "hdfs://master");

    // hbase的zookeeper地址
    conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4");

    // MapReduce任务提交到集群运行
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.hostname", "hadoop1");
    conf.set("mapreduce.job.jar", "E:\\project\\hadoop\\out\\artifacts\\hadoop_jar\\hadoop.jar");
    conf.set("mapreduce.app-submission.cross-platform", "true");    // 意思是跨平台提交，在windows下如果没有这句代码会报错
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    conf.set("dfs.client.use.datanode.hostname", "true");           // 使 NameNode 返回 DataNode 的主机名而不是 IP

    Connection conn = ConnectionFactory.createConnection(conf);
    TableName name = TableName.valueOf("stu1");
    Admin admin = conn.getAdmin();

    if (!admin.tableExists(name)) {

      // 表描述
      HTableDescriptor desc = new HTableDescriptor(name);

      // 列簇
      HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));

      info.setMaxVersions(3);

      HColumnDescriptor school = new HColumnDescriptor(Bytes.toBytes("school"));

      school.setMaxVersions(3);
      desc.addFamily(info);
      desc.addFamily(school);
      admin.createTable(desc);
    }

    admin.close();
    conn.close();

    Job job = Job.getInstance(conf);

    job.setJarByClass(ReadHbaseToHDFS.class);
    job.setJobName("ReadHdfsToHbase");
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    Scan scan = new Scan();

    TableMapReduceUtil.initTableReducerJob("stu1", MyReducer.class, job);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Mutation.class);
    FileInputFormat.setInputPaths(job, new Path("hdfs://master/input/stu"));

    // 启动job
    // job.submit();//无日志
    job.waitForCompletion(true);
  }

  /**
   * Class description
   *
   * @author wyg0405 @gmail.com
   * @version 1.0.0
   * @date
   * @since JDK1.8
   */
  public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    /**
     * Method description
     *
     * @param key     the key
     * @param value   the value
     * @param context the context
     * @throws IOException          the io exception
     * @throws InterruptedException the interrupted exception
     * @author
     * @date 2019.03.20 22:44:04
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(value, NullWritable.get());
    }
  }


  /**
   * Class description
   *
   * @author wyg0405 @gmail.com
   * @version V1.0.0, 19/03/21
   */
  public static class MyReducer extends TableReducer<Text, NullWritable, NullWritable> {
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
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
      String[] split = key.toString().split("\t");
      Put put = new Put(Bytes.toBytes(split[0]));

      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(split[2]));
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(split[3]));
      put.addColumn(Bytes.toBytes("school"), Bytes.toBytes("major"), Bytes.toBytes(split[4]));
      context.write(NullWritable.get(), put);
    }
  }
}

