package com.wyg.hadoop.day04customizeclass;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DESCRIPTION:流量实体类
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 19:26
 **/

public class FlowEntity implements Writable {

  private int upload;
  private int download;
  private int totalFlow;

  public FlowEntity() {
  }

  public FlowEntity(int upload, int download) {
    this.upload = upload;
    this.download = download;
    totalFlow = this.upload + this.download;
  }

  public int getUpload() {
    return upload;
  }

  public void setUpload(int upload) {
    this.upload = upload;
  }

  public int getDownload() {
    return download;
  }

  public void setDownload(int download) {
    this.download = download;
  }

  public int getTotalFlow() {
    return totalFlow;
  }

  public void setTotalFlow(int totalFlow) {
    this.totalFlow = totalFlow;
  }

  @Override
  public String toString() {
    return
            "\t" + upload +
                    "\t" + download +
                    "\t" + totalFlow;
  }

  //序列化
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(upload);
    dataOutput.writeInt(download);
    dataOutput.writeInt(totalFlow);
  }

  //反序列化
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    upload = dataInput.readInt();
    download = dataInput.readInt();
    totalFlow = dataInput.readInt();
  }
}
