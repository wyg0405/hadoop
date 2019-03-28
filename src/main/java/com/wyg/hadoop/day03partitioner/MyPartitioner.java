package com.wyg.hadoop.day03partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * DESCRIPTION:自定义分区
 * 例如a-j为一个分区，k-z为一个分区
 * Partitioner<KEY, VALUE>
 * KEY,map输出的key类型
 * VALUE,map输出的value类型
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 14:42
 **/

public class MyPartitioner extends Partitioner<Text, IntWritable> {

  /**
   * @param text        map输出的key
   * @param intWritable map输出的value
   * @param i           分区个数
   * @return
   */
  @Override
  public int getPartition(Text text, IntWritable intWritable, int i) {
    String key = text.toString();
    key = key.replaceAll(" ", "");

    if ("" != key && key != null) {
      char c = key.charAt(0);
      if (c >= 'a' && c <= 'j') {
        return 0;
      } else {
        return 1;
      }
    } else {
      return 0;
    }


  }
}
