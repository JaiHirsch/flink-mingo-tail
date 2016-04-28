package org.flinkmon.mongo.conn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.WebServiceException;

import com.mongodb.ConnectionString;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.sun.xml.internal.ws.Closeable;

public class ShardSetFinder implements Closeable {

   private List<MongoClientWrapper> mongoClients;

   public Map<String, List<MongoClientWrapper>> findShardSets(MongoClient mongoS) {
      // TODO figure out how to do this with the new driver syntax. Does not appear to support sisterDB
      DBCursor find = mongoS.getDB("admin").getSisterDB("config").getCollection("shards").find();
      Map<String, List<MongoClientWrapper>> shardSets = new HashMap<>();
      while (find.hasNext()) {
         DBObject next = find.next();
         String key = (String) next.get("_id");
         shardSets.put(key, getMongoClient(buildServerAddressList(next)));
      }
      find.close();
      return shardSets;
   }

   private List<MongoClientWrapper> getMongoClient(List<ConnectionString> shardSet) {
      mongoClients = new ArrayList<>();
      try {
         for (ConnectionString address : shardSet) {
            com.mongodb.reactivestreams.client.MongoClient client = MongoClients.create(address);
            mongoClients.add(new MongoClientWrapper(address.getConnectionString(), client));
            Thread.sleep(100); // allow the client to establish prior to being
         }
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      return mongoClients;
   }

   private List<ConnectionString> buildServerAddressList(DBObject next) {
      List<ConnectionString> hosts = new ArrayList<>();
      for (String host : Arrays.asList(((String) next.get("host")).split("/")[1].split(","))) {
         hosts.add(new ConnectionString("mongodb://" + host));
      }
      return hosts;
   }

   @Override
   public void close() throws WebServiceException {
      for (MongoClientWrapper w : mongoClients) {
         w.getClient().close();
      }

   }
}
