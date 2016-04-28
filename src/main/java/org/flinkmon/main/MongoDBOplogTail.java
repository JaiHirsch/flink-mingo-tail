package org.flinkmon.main;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.bson.Document;
import org.flinkmon.elastic.ElasticsearchEmbeddedNodeSink;
import org.flinkmon.source.MongoDBOplogSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBOplogTail {
   private static Logger logger = LoggerFactory.getLogger(MongoDBOplogTail.class);

   public static void main(String[] args) throws Exception {
      logger.info("--------------- Beginning MongoDB oplog tailing to Flink -------------");

      StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
      DataStream<Document> ds = see.addSource(new MongoDBOplogSource("localhost", 27017));
      ds.addSink(new PrintSinkFunction<Document>());
      ds.addSink(new ElasticsearchEmbeddedNodeSink("elasticsearch_jaihirsch").getElasticSink());
      see.execute();
   }

}
//
// curl -XPUT 'localhost:9200/grades?pretty'
// curl 'localhost:9200/_cat/indices?v'
