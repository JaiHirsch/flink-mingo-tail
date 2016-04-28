package org.flinkmon.mongo.conn;

import com.mongodb.reactivestreams.client.MongoClient;

public class MongoClientWrapper {
   
   private final String host;
   private final MongoClient client;

   public MongoClientWrapper(String host, MongoClient client) {
      this.host = host;
      this.client = client;
   }

   public String getHost() {
      return host;
   }

   public MongoClient getClient() {
      return client;
   }
   
   

}
