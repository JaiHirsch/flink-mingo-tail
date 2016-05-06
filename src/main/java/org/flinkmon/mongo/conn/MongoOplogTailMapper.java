package org.flinkmon.mongo.conn;

/**
 * 
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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.ne;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.reactivestreams.client.FindPublisher;

import static org.flinkmon.mongo.conn.MongoDBConstants.*;

public class MongoOplogTailMapper {
   private final String host;
   private final int port;
   private Integer replicaDepth;
   Logger logger = LoggerFactory.getLogger(this.getClass());
   public MongoOplogTailMapper(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public Map<String, FindPublisher<Document>> establishMongoPublishers(
         MongoCollection<Document> tsCollection) {
      Map<String, FindPublisher<Document>> publishers = new HashMap<>();
      try (MongoClient mongoS = new MongoClient(host, port)) {
         for (List<MongoClientWrapper> clients : new ShardSetFinder().findShardSets(mongoS)
               .values()) {
            if (null == getReplicaDepth()) setReplicaDepth(clients.size());
            bindHostToPublisher(tsCollection, publishers, clients);
         }
      }
      return publishers;
   }

   private void bindHostToPublisher(MongoCollection<Document> tsCollection,
         Map<String, FindPublisher<Document>> publishers, List<MongoClientWrapper> clients) {
      for (MongoClientWrapper client : clients) {
         logger.info("------------ Binding "+client.getHost()+" to oplog. ---------------");
         FindPublisher<Document> oplogPublisher = client.getClient().getDatabase("local")
               .getCollection("oplog.rs").find().filter(getQueryFilter(tsCollection, client))
               .sort(new Document("$natural", 1)).cursorType(CursorType.TailableAwait);
         publishers.put(client.getHost(), oplogPublisher);
      }
   }

   private Bson getFilterLastTimeStamp(MongoCollection<Document> tsCollection,
         MongoClientWrapper client) {
      Document lastTimeStamp = tsCollection.find(new Document("_id", client.getHost())).limit(1)
            .first();
      return getTimeQuery(lastTimeStamp == null ? null : (BsonTimestamp) lastTimeStamp
            .get(OPLOG_TIMESTAMP));
   }

   private Bson getQueryFilter(MongoCollection<Document> tsCollection, MongoClientWrapper client) {
      return and(ne(OPLOG_NAME_SPACE, "time_d.repl_time"),
            ne(OPLOG_OPERATION, OPLOG_OPERATION_NO_OP), exists("fromMigrate", false),
            getFilterLastTimeStamp(tsCollection, client));
   }

   private Bson getTimeQuery(BsonTimestamp lastTimeStamp) {
      return lastTimeStamp == null ? new Document() : gt(OPLOG_TIMESTAMP, lastTimeStamp);
   }

   public Integer getReplicaDepth() {
      return replicaDepth;
   }

   public void setReplicaDepth(Integer replicaDepth) {
      this.replicaDepth = replicaDepth;
   }
}
