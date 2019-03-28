package com.wyg.hadoop.day06course2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 21:47
 **/

public class CourseMapper extends Mapper<LongWritable, Text, CourseEntity, Student> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] line = value.toString().split("\t");

    context.write(new CourseEntity(line[0], Integer.parseInt(line[2])), new Student(line[1]));
  }
}
