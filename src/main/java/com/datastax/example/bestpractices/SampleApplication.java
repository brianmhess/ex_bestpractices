package com.datastax.example.bestpractices;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import reactor.core.publisher.Flux;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public class SampleApplication {
    public static void main(String[] args) {
        final String dc = "dc1";
        final String ks = "ks";
        final String tbl = "tbl";
        final int ddlRetries = 3;
        final int ddlRetrySleepMs = 500;
        // Create CqlSession
        CqlSession session = CqlSession.builder()
//                .withAuthCredentials("cass_user", "choose_a_better_password")  // redundant with application.conf, but showing for demonstration
//                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))     // redundant with application.conf, but showing for demonstration
                .withLocalDatacenter("dc1")
                .build();

        // Set up driver metrics and expose via JMX (as an example)
        MetricRegistry registry = session.getMetrics()
                .orElseThrow(() -> new IllegalStateException("Metrics are disabled"))
                .getRegistry();

        JmxReporter reporter =
                JmxReporter.forRegistry(registry)
                        .inDomain("com.datastax.oss.driver")
                        .build();
        reporter.start();

        // Create Keyspace
        SimpleStatement createKeyspace = SimpleStatement.newInstance("CREATE KEYSPACE IF NOT EXISTS "+ks
                +" WITH replication = {'class': 'NetworkTopologyStrategy', '" + dc + "': '1'}");
        // Synchronous
        ResultSet ddl = null;
        try {
            ddl = session.execute(createKeyspace);
        }
        catch (Exception e) {
            throw new RuntimeException("Error creating keyspace");
        }
        if (!ddl.getExecutionInfo().isSchemaInAgreement()) {
            int retries = 0;
            while ((retries < ddlRetries) && session.checkSchemaAgreement()) {
                try {
                    Thread.sleep(ddlRetrySleepMs);
                }
                catch (InterruptedException ie) {
                    throw new RuntimeException("Interrupted while waiting for schema agreement");
                }
                retries++;
            }
        }

        // Create Table (using helper method)
        SimpleStatement createTable = SimpleStatement.newInstance("CREATE TABLE IF NOT EXISTS "+ks+"."+tbl
                +"(pkey INT, x INT, PRIMARY KEY ((pkey)))");
        if (!executeDdl(session, createTable, ddlRetries, ddlRetrySleepMs)) {
            System.err.println("Error creating table");
            System.exit(1);
        }

        // Insert some data
        // Simple
        SimpleStatement stmt1 = SimpleStatement.newInstance("INSERT INTO "+ks+"."+tbl+"(pkey,x) VALUES (1,2)");
        stmt1 = stmt1.setIdempotent(true);
        try {
            session.execute(stmt1);
        }
        catch (QueryExecutionException | QueryValidationException | AllNodesFailedException ex) {
            // Handle query failure
            throw new RuntimeException("Error inserting first data");
        }

        // Prepared
        // Using positional bind markers
        SimpleStatement stmt2 = SimpleStatement.newInstance("INSERT INTO "+ks+"."+tbl+"(pkey,x) VALUES (?,?)");
        PreparedStatement prepared =  session.prepare(stmt2);
        BoundStatement bound1 = prepared.bind();
        bound1 = bound1.setInt(0, 10);
        bound1 = bound1.setInt(1, 20);
        // Async
        try {
            CompletionStage<AsyncResultSet> asyncResult = session.executeAsync(bound1);
            asyncResult.toCompletableFuture().get();
        }
        catch (Exception e) {
            // process exception
        }

        // Reactive
        // Using named bind markers instead of positional
        SimpleStatement stmt3 = SimpleStatement.newInstance("INSERT INTO "+ks+"."+tbl+"(pkey,x) VALUES (:pkey,:x)");
        PreparedStatement prepared2 = session.prepare(stmt3);
        BoundStatement bound2 = prepared2.bind();
        bound2 = bound2.setInt("pkey", 100);
        bound2 = bound2.setInt("x", 200);
        bound2 = bound2.unset(1); // Showing for demonstration
        bound2 = bound2.setIdempotent(true); // set idempotency
        bound2 = bound2.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        ReactiveResultSet reactiveResult = session.executeReactive(bound2);
        Flux.from(reactiveResult).blockLast();

        // Batch
        BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
        BoundStatement bound3 = prepared.bind(1000,2000);
        BoundStatement bound4 = prepared2.bind().setInt("pkey",10000).setInt("x", 20000);
        batchBuilder.addStatement(bound3);
        batchBuilder.addStatement(bound4);
        BatchStatement batch = batchBuilder.build();
        try {
            session.execute(batch);
        }
        catch (QueryExecutionException qee) {
            // Handle query timeout - let's retry it
            try {
                session.execute(batch);
            }
            catch (QueryExecutionException | QueryValidationException | AllNodesFailedException ex) {
                // Handle query failure
                throw new RuntimeException("Error second try inserting data");
            }

        }
        catch (QueryValidationException | AllNodesFailedException ex) {
            // Handle query failure
            throw new RuntimeException("Error inserting data");
        }


        // Query Builder
        SimpleStatement read = QueryBuilder.selectFrom(ks, tbl)
                .columns("pkey", "x").build();
        // Using a helper method to execute DML
        ResultSet resultSet = executeDml(session, read, "Error reading data");
        if (null != resultSet) {
            for (Row row : resultSet) {
                System.out.println("pkey: " + row.getInt("pkey") + ", x: " + row.getInt("x"));
            }
        }

        // Cleanup
        session.close();
    }

    private static boolean executeDdl(CqlSession session, Statement ddlStatement, int ddlRetries, int ddlRetrySleepMs) {
        ResultSet ddl = null;
        try {
            ddl = session.execute(ddlStatement);
        }
        catch (Exception e) {
            throw new RuntimeException("Exception while executing DDL (" + ddlStatement + ")");
        }
        if (!ddl.getExecutionInfo().isSchemaInAgreement()) {
            int retries = 0;
            while ((retries < ddlRetries) && session.checkSchemaAgreement()) {
                try {
                    Thread.sleep(ddlRetrySleepMs);
                }
                catch (InterruptedException ie) {
                    throw new RuntimeException("Interrupted while waiting for schema agreement");
                }
                retries++;
            }
        }
        return true;
    }

    private static ResultSet executeDml(CqlSession session, Statement query, String errorString) {
        ResultSet resultSet = null;
        try {
            resultSet = session.execute(query);
        }
        catch (QueryExecutionException | QueryValidationException | AllNodesFailedException ex) {
            // Handle query failure
            throw new RuntimeException(errorString);
        }
        return resultSet;
    }
}
