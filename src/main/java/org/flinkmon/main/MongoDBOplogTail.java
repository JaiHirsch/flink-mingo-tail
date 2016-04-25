package org.flinkmon.main;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.bson.Document;
import org.flinkmon.source.MongoDBOplogSource;

public class MongoDBOplogTail {

   public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
      DataStream<Document> ds = see.addSource(new MongoDBOplogSource()) ;
      
      ds.addSink(new PrintSinkFunction<Document>());
//      result.print();

      see.execute();
   }
}
