package hessian.example;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import reactor.core.publisher.Flux;

import java.net.InetSocketAddress;

public class SampleApplication {
    public static void main(String[] args) {
        final String dc = "dc1";
        final String ks = "ks";
        final String tbl = "tbl";
        // Create CqlSession
        CqlSession session = CqlSession.builder()
//                .withAuthCredentials("cass_user", "choose_a_better_password")  // redundant with application.conf, but showing for demonstration
//                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))     // redundant with application.conf, but showing for demonstration
                .withLocalDatacenter("dc1")
                .build();

        // Create Keyspace and Table
        SimpleStatement createKeyspace = SimpleStatement.newInstance("CREATE KEYSPACE IF NOT EXISTS "+ks
                +" WITH replication = {'class': 'NetworkTopologyStrategy', '" + dc + "': '1'}");
        // Synchronous
        session.execute(createKeyspace);
        SimpleStatement createTable = SimpleStatement.newInstance("CREATE TABLE IF NOT EXISTS "+ks+"."+tbl
                +"(pkey INT, x INT, PRIMARY KEY ((pkey)))");
        session.execute(createTable);

        // Insert some data
        // Simple
        SimpleStatement stmt1 = SimpleStatement.newInstance("INSERT INTO "+ks+"."+tbl+"(pkey,x) VALUES (1,2)");
        session.execute(stmt1);

        // Prepared
        SimpleStatement stmt2 = SimpleStatement.newInstance("INSERT INTO "+ks+"."+tbl+"(pkey,x) VALUES (?,?)");
        PreparedStatement prepared =  session.prepare(stmt2);
        BoundStatement bound1 = prepared.bind();
        bound1 = bound1.setInt(0, 10);
        bound1 = bound1.setInt(1,20);
        // Async
        try {
            session.executeAsync(bound1).toCompletableFuture().get();
        }
        catch (Exception e) {
            // process exception
        }

        // Reactive
        BoundStatement bound2 = prepared.bind();
        bound2 = bound2.setInt(0, 100);
        bound2 = bound2.unset(1); // redundant, but showing for demonstration
        bound2 = bound2.setIdempotent(true); // set idempotency
        bound2 = bound2.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        Flux.from(session.executeReactive(bound2)).blockLast();

        // Batch
        BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
        BoundStatement bound3 = prepared.bind(1000,2000);
        BoundStatement bound4 = prepared.bind(10000,20000);
        batchBuilder.addStatement(bound3);
        batchBuilder.addStatement(bound4);
        BatchStatement batch = batchBuilder.build();
        session.execute(batch);

        // Query Builder
        SimpleStatement read = QueryBuilder.selectFrom(ks, tbl)
                .columns("pkey", "x").build();
        ResultSet resultSet = session.execute(read);
        for (Row row : resultSet) {
            System.out.println("pkey: " + row.getInt("pkey") + ", x: " + row.getInt("x"));
        }

        // Cleanup
        session.close();
        return;
    }
}
