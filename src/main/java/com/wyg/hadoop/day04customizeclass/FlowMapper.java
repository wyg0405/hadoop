package com.wyg.hadoop.day04customizeclass;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 20:18
 **/

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowEntity> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] words = line.split("\t");

    context.write(new Text(words[1]), new FlowEntity(Integer.parseInt(words[words.length - 3]), Integer.parseInt(words[words.length - 2])));
  }
}
