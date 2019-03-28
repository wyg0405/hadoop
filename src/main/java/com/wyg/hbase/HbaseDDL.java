package com.wyg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Hbase DDL操作
 *
 * @author wyg0405@gmail.com
 * @create 2019-03-13 10:18
 **/

public class HbaseDDL {
  public static Configuration conf = null;
  public static Connection connection = null;
  public static Admin admin = null;
  private Table table = null;

  /**
   * 初始化，创建连接
   *
   * @throws IOException
   */
  @Before
  public void init() throws IOException {
    //配置文件
    conf = HBaseConfiguration.create();
    //zookeeper地址
    conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4");
    //创建连接
    connection = ConnectionFactory.createConnection(conf);

    System.out.println(connection.toString());

    admin = connection.getAdmin();

    table = connection.getTable(TableName.valueOf("test1"));
  }

  /**
   * 关闭连接
   *
   * @throws IOException
   */
  @After
  public void destroy() throws IOException {

    admin.close();
    table.close();
    connection.close();
  }

  @Test
  public void createTable() throws IOException {
    //表名
    TableName tableName = TableName.valueOf("test1");

    //表描述
    HTableDescriptor descriptor = new HTableDescriptor(tableName);

    //列簇info
    HColumnDescriptor info = new HColumnDescriptor("info");
    info.setMaxVersions(3);
    descriptor.addFamily(info);

    //列簇score
    HColumnDescriptor score = new HColumnDescriptor("score");
    score.setMaxVersions(3);
    descriptor.addFamily(score);

    //创建table
    admin.createTable(descriptor);

    System.out.printf(admin.getTableDescriptor(tableName).toString());
  }

  /**
   * 删除表
   *
   * @throws IOException
   */
  @Test
  public void deleteTable() throws IOException {
    TableName name = TableName.valueOf("test1");
    admin.disableTable(name);
    admin.deleteTable(name);
  }

  /**
   * 添加列簇
   *
   * @throws IOException
   */
  @Test
  public void addColumn() throws IOException {
    TableName name = TableName.valueOf("test1");
    HColumnDescriptor desc = new HColumnDescriptor("work");
    admin.addColumn(name, desc);
    System.out.println(admin.getTableDescriptor(name).toString());
  }

  /**
   * 删除列簇
   *
   * @throws IOException
   */
  @Test
  public void deleteColumn() throws IOException {
    TableName name = TableName.valueOf("test1");
    admin.deleteColumn(name, "work".getBytes());
    System.out.println(admin.getTableDescriptor(name).toString());
  }

}
