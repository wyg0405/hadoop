package com.wyg.hadoop.day05course;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 21:56
 **/

public class CourseReducer extends Reducer<Text, CourseEntity, Text, CourseEntity> {
  @Override
  protected void reduce(Text key, Iterable<CourseEntity> values, Context context) throws IOException, InterruptedException {
    int max = 0;
    int min = 100;
    float avg = 0;
    float sum = 0;
    int count = 0;
    for (CourseEntity courseEntity : values) {
      System.out.println(courseEntity.getScore());
      sum += courseEntity.getScore();
      if (max < courseEntity.getScore()) {
        max = courseEntity.getScore();
      }
      if (min > courseEntity.getScore()) {
        min = courseEntity.getScore();
      }
      count++;
      System.out.println(key.toString() + "\t" + sum + "\t" + max + "\t" + min + "\t" + count);
    }
    avg = sum / count;
    context.write(key, new CourseEntity(max, min, avg));
  }
}
