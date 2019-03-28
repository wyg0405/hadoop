package com.wyg.hadoop.day02mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * DESCRIPTION:
 * redcuer处理来自mapper的数据
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN：Reducer的key输入类型，Mapper输出的key，Text
 * VALUEIN ：Reducer的value输入类型,Mapper输出的值类型
 * KEYOUT:Reducer的key输出类型
 * VALUEOUT:Reducer的value输出类型
 *
 * @author wyg0405@gmail.com
 * @create 2019-01-24 21:51
 **/

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * redcuer处理数据前，相同的单词最好在一起，事实上确实在一起
   * mapper的数据在到达reducer前，框架会对数据进行整理，这个过程叫分组
   *
   * @param key     每一组相同的key
   * @param values  每一组相同key所对应的所有值<1,1,1,1,1,...>
   * @param context 写到hdfs
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values
    ) {

      sum += value.get();

    }
    context.write(key, new IntWritable(sum));
  }
}
