package com.wyg.hadoop.day09._3product;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wyg0405@gmail.com
 * @description: goods.txt
 * 1	0	电器
 * 2	0	食品
 * 3	0	服饰
 * 4	1	冰箱
 * 5	1	彩电
 * 6	2	肉类
 * 7	2	蔬菜
 * 8	3	鞋子
 * 9	3	裤子
 * 10	4	海尔
 * 11	4	美的
 * 12	5	创维
 * 13	5	海信
 * 14	6	牛肉
 * 15	6	羊肉
 * 16	7	白菜
 * 17	7	空心菜
 * 18	8	耐克
 * 19	8	阿迪达斯
 * 20	9	才子
 * 21	9	海澜之家
 * <p>
 * 结果
 * 10	0	电器-冰箱-海尔
 * 11	0	电器-冰箱-美的
 * 12	0	电器-彩电-创维
 * 13	0	电器-彩电-海信
 * 14	0	食品-肉类-牛肉
 * 15	0	食品-肉类-羊肉
 * 16	0	食品-蔬菜-白菜
 * 17	0	食品-蔬菜-空心菜
 * 18	0	服饰-鞋子-耐克
 * 19	0	服饰-鞋子-阿迪达斯
 * 20	0	服饰-裤子-才子
 * 21	0	服饰-裤子-海澜之家
 * @date 2019-03-01 下午2:55
 */
public class Product {

  public static void main(String[] args)
          throws IOException, ClassNotFoundException, InterruptedException {
    // 加载配置文件
    Configuration conf = new Configuration();
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
    // 使 NameNode 返回 DataNode 的主机名而不是 IP
    conf.set("dfs.client.use.datanode.hostname", "true");
    int count = 4;
    for (int i = 0; i < 2; i++) {
      // 启动一个Job
      Job job = Job.getInstance(conf);

      // 计算程序主驱动类
      job.setJarByClass(Product.class);

      // 设置Mapper及其key,value类型
      job.setMapperClass(MyMapper.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);

      // 设置reducer及其key,value类型
      job.setReducerClass(MyReducer.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);

      // 输入路径
      if (i == 0) {
        FileInputFormat.addInputPath(job, new Path("/input/goods"));
      } else {
        FileInputFormat.addInputPath(
                job, new Path("/output/goods/" + count + "/" + (i - 1)));
      }

      // 输出路径,该路径不能存在
      FileOutputFormat.setOutputPath(job, new Path("/output/goods/" + count + "/" + i));

      // 启动job
      // job.submit();//无日志
      job.waitForCompletion(true);
    }
  }

  public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    IntWritable k = new IntWritable();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String[] lines = value.toString().split("\t");
      if ("0".equals(lines[1])) {
        k.set(Integer.parseInt(lines[0]));
        v.set(lines[1] + "\t" + lines[2]);
      } else {
        k.set(Integer.parseInt(lines[1]));
        v.set(lines[0] + "\t" + lines[2]);
      }
      context.write(k, v);
    }
  }

  public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    IntWritable k = new IntWritable();
    Text v = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      String parent = "";
      List<String> children = new ArrayList<String>();

      for (Text value : values) {
        if (value.toString().startsWith("0")) {
          parent = value.toString();
        } else {
          children.add(value.toString());
        }
      }

      if (children.size() == 0) {
        v.set(parent);
        context.write(key, v);
      } else if ("".equals(parent)) {
        for (String child : children) {
          k.set(Integer.parseInt(child.split("\t")[0]));
          v.set(key.get() + "\t" + child.split("\t")[1]);
          context.write(k, v);
        }
      } else {
        for (String child : children) {
          k.set(Integer.parseInt(child.split("\t")[0]));
          v.set(parent + "-" + child.split("\t")[1]);
          context.write(k, v);
        }
      }
    }
  }
}
