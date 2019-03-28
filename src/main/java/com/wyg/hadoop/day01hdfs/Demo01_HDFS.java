package com.wyg.hadoop.day01hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-01-19 16:26
 **/

public class Demo01_HDFS {

  public static void main(String[] args) throws InterruptedException {

    try {
      /**
       *1.conf在没有配置文件时默认加载jar包里的 hadoop-2.7.6\share\hadoop\day01hdfs\day01hdfs-default.xml
       * 2.若有hdfs配置文件，则读取该配置文件，且配置文件只能命名为hdfs-default.xml或hdfs-site.xml,否则手动加载配置文件conf.addResource("**.xml");
       * 3.也可以通过代码设置,例如 conf.set("dfs.replication","5");
       * 加载顺序jar包>>配置文件>>代码
       */
      Configuration conf = new Configuration();
      //conf.set("dfs.replication","5");
      conf.set("dfs.client.use.datanode.hostname", "true");


      //权限不足也可以在jvm配置参数:-DHADOOP_USER_NAME=hadoop
      //System.setProperty("HADOOP_USER_NAME","hadoop");
      FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "hadoop");

      Path local = new Path("C:\\Users\\wyg04\\Desktop\\1.txt");
      Path target = new Path("/1.txt");
      fs.copyFromLocalFile(local, target);
      fs.close();


    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }
}
