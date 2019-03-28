package com.wyg.hadoop.day09._2course;


import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-14 11:17
 **/

@SuppressWarnings({"ALL", "AlibabaCommentsMustBeJavadocFormat"})
public class CourseEntity implements WritableComparable<CourseEntity> {
  private String course;
  private double avg;

  public CourseEntity() {
  }

  public CourseEntity(String course, double avg) {
    this.course = course;
    this.avg = avg;
  }

  public String getCourse() {
    return course;
  }

  public void setCourse(String course) {
    this.course = course;
  }

  public double getAvg() {
    return avg;
  }

  public void setAvg(double avg) {
    this.avg = avg;
  }

  @Override
  public String toString() {
    return course +
            "\t" + avg;
  }

  //先对分组字段排序
  @Override
  public int compareTo(CourseEntity o) {
    int courseCompare = this.course.compareTo(o.getCourse());
    if (courseCompare == 0) {
      return this.avg - o.getAvg() > 0 ? -1 : (this.avg - o.getAvg() == 0 ? 0 : 1);
    } else {
      return courseCompare;
    }

  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(course);
    dataOutput.writeDouble(avg);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.course = dataInput.readUTF();
    this.avg = dataInput.readDouble();
  }
}

