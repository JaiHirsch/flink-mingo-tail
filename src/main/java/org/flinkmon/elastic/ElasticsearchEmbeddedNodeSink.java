package org.flinkmon.elastic;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder;
import org.bson.Document;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class ElasticsearchEmbeddedNodeSink {

   private final String clusterName;

   public ElasticsearchEmbeddedNodeSink(String clusterName) {
      this.clusterName = clusterName;

   }

   public ElasticsearchSink<Document> getElasticSink() {
      Map<String, String> config = Maps.newHashMap();
      config.put("bulk.flush.max.actions", "1");
      config.put("cluster.name", clusterName);
      config.put("discovery.zen.ping.multicast.enabled", "false");
      config.put("discovery.zen.ping.unicast.hosts", "localhost");

      return new ElasticsearchSink<>(config, new IndexRequestBuilder<Document>() {

         private static final long serialVersionUID = 5670092038059852584L;

         @Override
         public IndexRequest createIndexRequest(Document element, RuntimeContext ctx) {
            Map<String, Object> json = new HashMap<>();
            json.put("data", element);

            return Requests.indexRequest().index("grades").type("student").source(json);
         }
      });
   }

}
