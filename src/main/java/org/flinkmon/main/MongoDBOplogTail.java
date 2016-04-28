package org.flinkmon.main;

/**

 This file is part of flink-mongo-tail.

 flink-mongo-tail is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 flink-mongo-tail is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with flink-mongo-tail.  If not, see <http://www.gnu.org/licenses/>.

 @Author Jai Hirsch
 @github https://github.com/JaiHirsch/flink-mingo-tail

 */

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

