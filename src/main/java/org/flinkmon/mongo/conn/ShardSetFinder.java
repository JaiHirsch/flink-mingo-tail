package org.flinkmon.mongo.conn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.ConnectionString;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

public class ShardSetFinder {

   public Map<String, List<com.mongodb.reactivestreams.client.MongoClient>> findShardSets(
         MongoClient mongoS) {
      // TODO figure out how to do this with the new driver syntax. Does not
      // appear to support sisterDB
      DBCursor find = mongoS.getDB("admin").getSisterDB("config").getCollection("shards").find();
      Map<String, List<com.mongodb.reactivestreams.client.MongoClient>> shardSets = new HashMap<>();
      while (find.hasNext()) {
         DBObject next = find.next();
         System.out.println("Adding " + next);
         String key = (String) next.get("_id");
         shardSets.put(key, getMongoClient(buildServerAddressList(next)));
      }
      find.close();
      return shardSets;
   }

   private List<com.mongodb.reactivestreams.client.MongoClient> getMongoClient(
         List<ConnectionString> shardSet) {
      List<com.mongodb.reactivestreams.client.MongoClient> mongoClients = new ArrayList<>();
      try {
         for (ConnectionString address : shardSet) {
            mongoClients.add(MongoClients.create(address));
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
         hosts.add(new ConnectionString(host));
      }
      return hosts;
   }
}
