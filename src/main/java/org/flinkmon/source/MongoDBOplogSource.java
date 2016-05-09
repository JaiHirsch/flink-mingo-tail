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

import static org.flinkmon.mongo.conn.MongoDBConstants.OPLOG_ID;
import static org.flinkmon.mongo.conn.MongoDBConstants.OPLOG_TIMESTAMP;

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
import org.flinkmon.mongo.conn.MongoOplogTailMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.RxReactiveStreams;
import rx.schedulers.Schedulers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.FindPublisher;

public class MongoDBOplogSource extends RichSourceFunction<Document> {

   private static final long serialVersionUID = 1140284841495470127L;
   private volatile boolean isRunning = true;
   private BlockingQueue<Document> opsQueue = new ArrayBlockingQueue<Document>(128);
   ConcurrentMap<Long, AtomicInteger> documentCounter = new ConcurrentHashMap<Long, AtomicInteger>();
   private final String host;
   private final int port;
   private Integer replicaDepth;
   private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

   public MongoDBOplogSource(String host, int port) {
      this.host = host;
      this.port = port;

   }

   @Override
   public void run(SourceContext<Document> ctx) throws Exception {
      try (MongoClient timeClient = new MongoClient(host, port)) {
         MongoCollection<Document> tsCollection = timeClient.getDatabase("time_d").getCollection(
               "repl_time");
         MongoOplogTailMapper mongoOplogTailMapper = new MongoOplogTailMapper(host, port);
         Map<String, FindPublisher<Document>> publishers = mongoOplogTailMapper
               .establishMongoPublishers(tsCollection);
         this.replicaDepth = mongoOplogTailMapper.getReplicaDepth();

         ExecutorService executor = Executors.newFixedThreadPool(publishers.size());
         for (Entry<String, FindPublisher<Document>> publisher : publishers.entrySet()) {
            bindPublisherToObservable(publisher, executor, tsCollection);
         }
         while (isRunning) {
            Document operation = opsQueue.poll(5, TimeUnit.SECONDS);
            if (operation == null) continue;
            ctx.collect(operation);
         }
         logger.info("!!!!!!!!!!!!!!!!! exiting data poll isRunning = " + isRunning);
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
                  logger.error(e.getMessage());
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
      try {
         Long opKey = t.getLong(OPLOG_ID);
         documentCounter.putIfAbsent(opKey, new AtomicInteger(1));
         if (documentCounter.get(opKey).getAndIncrement() >= replicaDepth) {
            opsQueue.put(t);
            documentCounter.remove(opKey);
         }
      } catch (Exception e) {
         e.printStackTrace();
         logger.error(e.getMessage());
      }
   }

   private void updateHostOperationTimeStamp(MongoCollection<Document> tsCollection,
         BsonTimestamp lastTimeStamp, String host) {
      try {
         tsCollection.replaceOne(new Document("_id", host),
               new Document("_id", host).append(OPLOG_TIMESTAMP, lastTimeStamp),
               (new UpdateOptions()).upsert(true));
      } catch (Exception e) {
         logger.error(e.getMessage());
      }
   }

   @Override
   public void cancel() {
      isRunning = false;

   }

}
