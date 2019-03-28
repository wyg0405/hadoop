package com.wyg.hadoop.day02mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * DESCRIPTION:
 * 对Mpper四个参数的说明Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN：输入的key的类型,每一行的起始偏移量
 * VALUEIN：输入的值类型，这里指一行的内容
 * KEYOUT：输出的key类型
 * VALUEOUT：输出的值类型
 *
 * @author wyg0405@gmail.com
 * @create 2019-01-24 16:24
 **/

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /**
   * map方法每一行调用一次
   *
   * @param key     每一行的内容
   * @param value   每一行的起始偏移量
   * @param context 上下文对象，用于传输
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //获取每一行内容，进行单词切分，并打上标签1
    String line = value.toString();
    String[] words = line.split("");
    for (String word : words
    ) {
      context.write(new Text(word), new IntWritable(1));
    }
  }
}
