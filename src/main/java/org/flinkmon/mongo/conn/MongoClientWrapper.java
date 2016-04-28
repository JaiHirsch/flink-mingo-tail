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
