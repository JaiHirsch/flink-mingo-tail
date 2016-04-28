package org.flinkmon.source;

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

   /**
    * 
    */
   private static final long serialVersionUID = 1140284841495470127L;
   private volatile boolean isRunning = true;
   private BlockingQueue<Document> opsQueue = new ArrayBlockingQueue<Document>(128);
   ConcurrentMap<Long, AtomicInteger> documentCounter = new ConcurrentHashMap<Long, AtomicInteger>();
   private final String host;
   private final int port;

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

   private void bindPublisherToObservable(Entry<String, FindPublisher<Document>> publisher,
         ExecutorService executor, MongoCollection<Document> tsCollection) {
      RxReactiveStreams
            .toObservable(publisher.getValue())
            .subscribeOn(Schedulers.from(executor))
            .subscribe(
                  t -> {
                     try {

                        putOperationOnOpsQueue(publisher, tsCollection, t);

                     } catch (InterruptedException e) {
                        e.printStackTrace();
                     }
                  });
   }

   private void putOperationOnOpsQueue(Entry<String, FindPublisher<Document>> publisher,
         MongoCollection<Document> tsCollection, Document t) throws InterruptedException {
      Long opKey = t.getLong("h");
      BsonTimestamp lastTimeStamp = t.get("ts", BsonTimestamp.class);
      documentCounter.putIfAbsent(opKey, new AtomicInteger(0));
      int opCounter = documentCounter.get(opKey).getAndIncrement();
      String host = publisher.getKey();
      tsCollection.replaceOne(new Document("host", host), new Document("host",
            host).append("ts", lastTimeStamp), (new UpdateOptions()).upsert(true));
      if (opCounter == 2) {

         opsQueue.put(t);
         documentCounter.remove(opKey);
      }
   }

   private Map<String, FindPublisher<Document>> establishMongoPublishers(
         MongoCollection<Document> tsCollection) {
      Map<String, FindPublisher<Document>> publishers = new HashMap<>();
      try (MongoClient mongoS = new MongoClient(host, port)) {
         for (List<MongoClientWrapper> clients : new ShardSetFinder().findShardSets(mongoS)
               .values()) {
            bindHostToPublisher(tsCollection, publishers, clients);
         }
      }
      return publishers;
   }

   private void bindHostToPublisher(MongoCollection<Document> tsCollection,
         Map<String, FindPublisher<Document>> publishers, List<MongoClientWrapper> clients) {
      for (MongoClientWrapper client : clients) {
         FindPublisher<Document> publisher = client.getClient().getDatabase("local")
               .getCollection("oplog.rs").find().filter(getQueryFilter(tsCollection, client))
               .sort(new Document("$natural", 1)).cursorType(CursorType.TailableAwait);
         publishers.put(client.getHost(), publisher);
      }
   }

   private Bson getFilterLastTimeStamp(MongoCollection<Document> tsCollection,
         MongoClientWrapper client) {
      Document lastTimeStamp = tsCollection.find(new Document("host", client.getHost())).limit(1)
            .first();
      return getTimeQuery(lastTimeStamp == null ? null : (BsonTimestamp) lastTimeStamp.get("ts"));
   }

   private Bson getQueryFilter(MongoCollection<Document> tsCollection, MongoClientWrapper client) {
      return and(ne("ns", "time_d.repl_time"), ne("op", "n"), exists("fromMigrate", false),
            getFilterLastTimeStamp(tsCollection, client));
   }

   private Bson getTimeQuery(BsonTimestamp lastTimeStamp) {
      return lastTimeStamp == null ? new Document() : gt("ts", lastTimeStamp);
   }

   @Override
   public void cancel() {
      isRunning = false;

   }

}
