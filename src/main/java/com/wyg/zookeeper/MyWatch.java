package com.wyg.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author wyg0405@gmail.com
 * @description:
 * @date 2019-03-07 下午7:44
 **/

public class MyWatch {
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    String connectString = "118.24.210.134:2181,118.24.245.226:2181,118.24.210.42:2181,118.24.206.104:2181";
    ZooKeeper zk = new ZooKeeper(connectString, 5000, new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        System.out.println("=========create zk connection==========");
        String path = watchedEvent.getPath();
        Event.EventType type = watchedEvent.getType();
        Event.KeeperState state = watchedEvent.getState();
        System.out.println(path + "\t" + type + "\t" + state);
      }
    });

    /**
     * 注册监听
     * 1. 传入监听器
     * 2. zk.getChildren("/mynode",null),传null会回调创建zk时watch的process方法，但监听事件为null
     * 3. zk.getChildren("/mynode",true),传true代表使用创建zk时的监听器
     * 4. zk.getChildren("/mynode",false),传入false表示不使用监听器
     */
    zk.getChildren("/mynode", new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        System.out.println("============getChildren watch==========");
        String path = watchedEvent.getPath();
        Event.EventType type = watchedEvent.getType();
        Event.KeeperState state = watchedEvent.getState();
        System.out.println(path + "\t" + type + "\t" + state);
      }
    });

    //zk.create("/mynode/node7", "jjj".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.delete("/mynode/node6", -1);
    zk.close();
  }

}
