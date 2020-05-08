package org.apache.hadoop.mapreduce.rdma;

public class IndexRecord {
  public long startOffset;
  public long rawLength;
  public long partLength;

  public IndexRecord() { }

  public IndexRecord(long startOffset, long rawLength, long partLength) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
  }

  public String toString(){
    return "startOffset: " + this.startOffset + ", rawLength: " + this.rawLength + ", partLength: " + partLength + ";\n";
  }
}
