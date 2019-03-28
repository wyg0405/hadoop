package com.wyg.hadoop.day06course2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-13 11:29
 **/

public class Student implements Writable {
  private String student;
  private int count;

  public Student() {
  }

  public Student(String student) {
    this.student = student;
  }

  public Student(String student, int count) {
    this.student = student;
    this.count = count;
  }

  public String getStudent() {
    return student;
  }

  public void setStudent(String student) {
    this.student = student;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return count +
            "\t" + student;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(count);
    dataOutput.writeBytes(student);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    count = dataInput.readInt();
    student = dataInput.readLine();
  }
}
