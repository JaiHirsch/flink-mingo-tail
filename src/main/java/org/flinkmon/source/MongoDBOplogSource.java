package org.flinkmon.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.bson.Document;
import org.flinkmon.mongo.conn.ShardSetFinder;

import rx.RxReactiveStreams;
import rx.schedulers.Schedulers;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.sun.org.apache.xpath.internal.compiler.OpMap;

public class MongoDBOplogSource extends RichSourceFunction<Document> {

   /**
    * 
    */
   private static final long serialVersionUID = 1140284841495470127L;
   private volatile boolean isRunning = true;
   private BlockingQueue<Document> ops = new ArrayBlockingQueue<Document>(128);
   Map<Long, AtomicInteger> documentCounter = new ConcurrentHashMap<Long, AtomicInteger>();

   public MongoDBOplogSource() {

   }

   @Override
   public void run(SourceContext<Document> ctx) throws Exception {
      List<FindPublisher<Document>> publishers = establishMongoPublisher();

      ExecutorService executor = Executors.newFixedThreadPool(publishers.size());

      for (FindPublisher<Document> publisher : publishers) {
         bindPublisherToObservable(publisher, executor);
      }

      while (isRunning) {
         ctx.collect(ops.poll(1, TimeUnit.MINUTES));
      }

   }

   private void bindPublisherToObservable(FindPublisher<Document> publisher,
         ExecutorService executor) {
      RxReactiveStreams.toObservable(publisher).subscribeOn(Schedulers.from(executor))
            .subscribe(t -> {
               try {
                  Long opKey = t.getLong("h");
                  AtomicInteger opCount = documentCounter.get(opKey);
                  if (null == opCount) {
                     documentCounter.put(opKey, new AtomicInteger(1));
                  } else if (opCount.get() > 2) {
                     documentCounter.remove(opKey);
                     ops.put(t);

                  } else documentCounter.get(opKey).getAndIncrement();
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            });
   }

   private List<FindPublisher<Document>> establishMongoPublisher() {
      List<FindPublisher<Document>> publishers = new ArrayList<>();
      Map<String, List<com.mongodb.reactivestreams.client.MongoClient>> findShardSets = new ShardSetFinder()
            .findShardSets(new MongoClient("mongodb://localhost:27017"));
      for (List<com.mongodb.reactivestreams.client.MongoClient> clients : findShardSets.values()) {
         for (com.mongodb.reactivestreams.client.MongoClient client : clients) {
            MongoCollection<Document> collection = client.getDatabase("local").getCollection(
                  "oplog.rs");

            FindPublisher<Document> publisher = collection.find().filter(getFilterw())
                  .filter(getElectionFilter()).sort(new Document("$natural", 1))
                  .cursorType(CursorType.TailableAwait);
            publishers.add(publisher);
         }
      }
      return publishers;
   }

   @Override
   public void cancel() {
      isRunning = false;

   }

   private static Document getElectionFilter() {
      return new Document("op", new Document("$ne", "n"));
   }

   private static Document getFilterw() {
      return new Document("fromMigrate", new Document("$exists", false));
   }

}
