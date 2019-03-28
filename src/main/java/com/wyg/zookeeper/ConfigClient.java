package com.wyg.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author wyg0405@gmail.com
 * @description: 配置文件触发端
 * @date 2019-03-07 下午10:29
 **/

public class ConfigClient {
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    ZooKeeper zk = new ZooKeeper("hadoop1:2181,hadoop2:2181,hadoop3:2181,hadoop4:2181", 5000, null);
    zk.create("/config/node5", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.close();
  }

}
