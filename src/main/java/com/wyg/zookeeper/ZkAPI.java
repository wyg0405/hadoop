package com.wyg.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author wyg0405@gmail.com
 * @description:
 * @date 2019-03-07 下午12:00
 **/

public class ZkAPI {

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    String connectString = "118.24.210.134:2181,118.24.245.226:2181,118.24.210.42:2181,118.24.206.104:2181";
    //String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181,hadoop4:2181";服务器有内外网，这种方式会返回内网地址,连不上
    ZooKeeper zk = new ZooKeeper(connectString, 5000, new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        // TODO Auto-generated method stub
        System.out.println(" receive event : " + watchedEvent.getType().name());
      }
    });
    System.out.println(zk);
    //create
        /*String create = zk.create("/mynode/mynode5", "this is mynode2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(create);*/

    /*delete,version-1为当前版本,非空不能删除*/
    /*zk.delete("/mynode", -1);*/

    //set
       /* Stat stat = zk.setData("/mynode", "kskd".getBytes(), -1);
        System.out.println(stat);*/

    //get
    String getdata = zk.getData("/mynode", null, null).toString();
    System.out.println(getdata);

    //getChildren
    List<String> children = zk.getChildren("/mynode", null);
    System.out.println(children);
    for (String s : children) {
      System.out.println(s);
    }


    //exists
    Stat exists = zk.exists("/myssnode", null);
    System.out.println(exists);
    zk.close();
  }

}
