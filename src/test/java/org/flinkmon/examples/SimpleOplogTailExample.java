package org.flinkmon.examples;

import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.*;

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

// This is a simple oplog tail example that uses the basic MongoClient and not reactive steams.

public class SimpleOplogTailExample {

    public static final String OPLOG_OPERATION = "op";
    public static final String OPLOG_OPERATION_NO_OP = "n";
    public static final String OPLOG_NAME_SPACE = "ns";

    public static void main(String[] args) {

        try (MongoClient client = new MongoClient()) {

            FindIterable<Document> oplogTail = client.getDatabase("local")
                    .getCollection("oplog.rs").find().filter(getQueryFilter())
                    .sort(new Document("$natural", 1)).cursorType(CursorType.TailableAwait);

            oplogTail.forEach((Block<Document>) document -> System.out.println(document));
        }
    }

    private static Bson getQueryFilter() {
        return and(ne(OPLOG_NAME_SPACE, "time_d.repl_time"),
                ne(OPLOG_OPERATION, OPLOG_OPERATION_NO_OP), exists("fromMigrate", false));
    }

}
