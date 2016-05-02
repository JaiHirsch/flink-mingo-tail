package org.flinkmon.source;

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
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.flinkmon.mongo.conn.MongoClientWrapper;
import org.flinkmon.mongo.conn.ShardSetFinder;

import rx.RxReactiveStreams;
import rx.schedulers.Schedulers;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.FindPublisher;

public class MongoDBOplogSource extends RichSourceFunction<Document> {

   private static final String OPLOG_OPERATION = "op";
   private static final String OPLOG_OPERATION_NO_OP = "n";
   private static final String OPLOG_NAME_SPACE = "ns";
   private static final String OPLOG_TIMESTAMP = "ts";
   private static final String OPLOG_ID = "h";
   private static final long serialVersionUID = 1140284841495470127L;
   private volatile boolean isRunning = true;
   private BlockingQueue<Document> opsQueue = new ArrayBlockingQueue<Document>(128);
   ConcurrentMap<Long, AtomicInteger> documentCounter = new ConcurrentHashMap<Long, AtomicInteger>();
   private final String host;
   private final int port;
   private Integer replicaDepth;

   public MongoDBOplogSource(String host, int port) {
      this.host = host;
      this.port = port;

   }

   @Override
   public void run(SourceContext<Document> ctx) throws Exception {
      try (MongoClient timeClient = new MongoClient(host, port)) {
         MongoCollection<Document> tsCollection = timeClient.getDatabase("time_d").getCollection(
               "repl_time");
         Map<String, FindPublisher<Document>> publishers = establishMongoPublishers(tsCollection);

         ExecutorService executor = Executors.newFixedThreadPool(publishers.size());
         for (Entry<String, FindPublisher<Document>> publisher : publishers.entrySet()) {
            bindPublisherToObservable(publisher, executor, tsCollection);
         }
         while (isRunning) {
            ctx.collect(opsQueue.poll(365, TimeUnit.DAYS));
         }
         executor.shutdownNow();
      }

   }

   private void bindPublisherToObservable(Entry<String, FindPublisher<Document>> oplogPublisher,
         ExecutorService executor, MongoCollection<Document> tsCollection) {
      RxReactiveStreams.toObservable(oplogPublisher.getValue())
            .subscribeOn(Schedulers.from(executor)).subscribe(t -> {
               try {

                  putOperationOnOpsQueue(oplogPublisher, tsCollection, t);

               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            });
   }

   private void putOperationOnOpsQueue(Entry<String, FindPublisher<Document>> publisher,
         MongoCollection<Document> tsCollection, Document t) throws InterruptedException {
      updateHostOperationTimeStamp(tsCollection, t.get(OPLOG_TIMESTAMP, BsonTimestamp.class),
            publisher.getKey());
      putOperationOnOpsQueueIfFullyReplicated(t);
   }

   private void putOperationOnOpsQueueIfFullyReplicated(Document t) throws InterruptedException {
      Long opKey = t.getLong(OPLOG_ID);
      documentCounter.putIfAbsent(opKey, new AtomicInteger(1));
      if (documentCounter.get(opKey).getAndIncrement() >= replicaDepth) {
         opsQueue.put(t);
         documentCounter.remove(opKey);
      }
   }

   private void updateHostOperationTimeStamp(MongoCollection<Document> tsCollection,
         BsonTimestamp lastTimeStamp, String host) {
      tsCollection.replaceOne(new Document("host", host),
            new Document("host", host).append(OPLOG_TIMESTAMP, lastTimeStamp),
            (new UpdateOptions()).upsert(true));
   }

   private Map<String, FindPublisher<Document>> establishMongoPublishers(
         MongoCollection<Document> tsCollection) {
      Map<String, FindPublisher<Document>> publishers = new HashMap<>();
      try (MongoClient mongoS = new MongoClient(host, port)) {
         for (List<MongoClientWrapper> clients : new ShardSetFinder().findShardSets(mongoS)
               .values()) {
            if (null == replicaDepth) replicaDepth = clients.size();
            bindHostToPublisher(tsCollection, publishers, clients);
         }
      }
      return publishers;
   }

   private void bindHostToPublisher(MongoCollection<Document> tsCollection,
         Map<String, FindPublisher<Document>> publishers, List<MongoClientWrapper> clients) {
      for (MongoClientWrapper client : clients) {
         FindPublisher<Document> oplogPublisher = client.getClient().getDatabase("local")
               .getCollection("oplog.rs").find().filter(getQueryFilter(tsCollection, client))
               .sort(new Document("$natural", 1)).cursorType(CursorType.TailableAwait);
         publishers.put(client.getHost(), oplogPublisher);
      }
   }

   private Bson getFilterLastTimeStamp(MongoCollection<Document> tsCollection,
         MongoClientWrapper client) {
      Document lastTimeStamp = tsCollection.find(new Document("host", client.getHost())).limit(1)
            .first();
      return getTimeQuery(lastTimeStamp == null ? null : (BsonTimestamp) lastTimeStamp.get(OPLOG_TIMESTAMP));
   }

   private Bson getQueryFilter(MongoCollection<Document> tsCollection, MongoClientWrapper client) {
      return and(ne(OPLOG_NAME_SPACE, "time_d.repl_time"), ne(OPLOG_OPERATION, OPLOG_OPERATION_NO_OP), exists("fromMigrate", false),
            getFilterLastTimeStamp(tsCollection, client));
   }

   private Bson getTimeQuery(BsonTimestamp lastTimeStamp) {
      return lastTimeStamp == null ? new Document() : gt(OPLOG_TIMESTAMP, lastTimeStamp);
   }

   @Override
   public void cancel() {
      isRunning = false;

   }

}
