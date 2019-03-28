package com.wyg.hadoop.day05course;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 21:39
 **/

public class CourseEntity implements Writable {
  private String course;
  private String student;
  private int score;
  private int max;
  private int min;
  private float avg;

  public CourseEntity() {
  }

  public CourseEntity(int max, int min, float avg) {
    this.max = max;
    this.min = min;
    this.avg = avg;
  }

  public CourseEntity(int score) {
    this.score = score;
  }

  public String getCourse() {
    return course;
  }

  public void setCourse(String course) {
    this.course = course;
  }

  public int getMax() {
    return max;
  }

  public void setMax(int max) {
    this.max = max;
  }

  public int getMin() {
    return min;
  }

  public void setMin(int min) {
    this.min = min;
  }

  public float getAvg() {
    return avg;
  }

  public void setAvg(float avg) {
    this.avg = avg;
  }

  public String getStudent() {
    return student;
  }

  public void setStudent(String student) {
    this.student = student;
  }

  public int getScore() {
    return score;
  }

  public void setScore(int score) {
    this.score = score;
  }

  @Override
  public String toString() {
    return course +
            "\t" + max +
            "\t" + min +
            "\t" + avg;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    //dataOutput.writeBytes(day05course);
    //dataOutput.writeBytes(student);
    dataOutput.writeInt(score);
    dataOutput.writeInt(max);
    dataOutput.writeInt(min);
    dataOutput.writeFloat(avg);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    //this.day05course = dataInput.readLine();
    //this.student = dataInput.readLine();
    score = dataInput.readInt();
    max = dataInput.readInt();
    min = dataInput.readInt();
    avg = dataInput.readFloat();
  }
}
