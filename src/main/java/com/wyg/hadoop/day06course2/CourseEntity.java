package com.wyg.hadoop.day06course2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DESCRIPTION:
 * 自定义Key
 * 必须有一个无参的构造函数。
 * <p>
 * 必须重写WritableComparable接口的hashCode()方法和equals()方法以及compareTo()方法。
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 21:39
 **/

public class CourseEntity implements WritableComparable<CourseEntity> {
  private String course;
  private int score;


  public CourseEntity() {
  }

  public CourseEntity(String course, int score) {
    this.course = course;
    this.score = score;
  }

  public String getCourse() {
    return course;
  }

  public void setCourse(String course) {
    this.course = course;
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
            "\t" + score;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(course);
    dataOutput.writeInt(score);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    course = dataInput.readUTF();
    score = dataInput.readInt();
  }


  //排序分组
  @Override
  public int compareTo(CourseEntity o) {
    if (!course.equals(o.getCourse())) {
      return course.compareTo(o.getCourse());
    } else if (score != o.getScore()) {
      return score < o.getScore() ? -1 : 1;
    } else {
      return 0;
    }
  }


  @Override
  public int hashCode() {
    return course.hashCode() + score;
  }

  @Override
  public boolean equals(Object obj) {

    if (!(obj instanceof CourseEntity)) {
      return false;
    } else {
      CourseEntity courseEntity = (CourseEntity) obj;
      if (course.equals(courseEntity.getCourse()) && score == courseEntity.getScore()) {
        return true;
      } else {
        return false;
      }
    }
  }
}
