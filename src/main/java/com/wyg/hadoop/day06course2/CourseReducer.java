package com.wyg.hadoop.day06course2;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 21:56
 **/

public class CourseReducer extends Reducer<CourseEntity, Student, CourseEntity, Student> {
  @Override
  protected void reduce(CourseEntity key, Iterable<Student> values, Context context) throws IOException, InterruptedException {
    System.out.println(key);
    int count = 0;
    String students = "";
    for (Student student : values) {
      students += "," + student.getStudent();
      count++;
    }
    students = students.replaceFirst(",", "");
    context.write(key, new Student(students, count));
  }
}
