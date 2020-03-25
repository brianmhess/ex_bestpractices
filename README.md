# DataStax Java Driver Best Practices Example
This example is a companion to the Best Practices for the DataStax Java Driver document.

## Editing the Config
You will need to edit the `src/main/resources/applications.conf` file with the
contact points for your cluster. The parameter `basic.contact-points` default to 
`127.0.0.1:9042` and `localhost:9042` 
(yes, that is redundant, but was done to illustrate how to specify multiple contact points).

You will also need to set the local data center. The default is `dc1`. If you need to change
that, you will need to change the value of `basic.load-balancing-policy.local-datacenter` in
`src/main/resources/application.conf`. 
In addition, you will need to edit the `SampleApplication.java` on the line where we set the
`dc` variable (line 13).

If you would like to rename the keyspace and/or table name for this example, you can edit the
assignment of the `ks` and/or `tbl` variables (on lines 14 and 15).

If your cluster requires a username and password, uncomment the line that reads:
```
.withAuthCredentials("cass_user", "choose_a_better_password")
```
and supply the username and password in the `withAuthCredentials()` call.

## Building
To build, run
``` 
mvn clean package
``` 

This will build the source into an executable jar including all its dependencies located at
``` 
./target/ex_bestpractices-0.1-SNAPSHOT.jar
```

## Running
To run this sample application, run:
``` 
java -jar target/ex_bestpractices-0.1-SNAPSHOT.jar
```

You should see output like the following:
```
cmdpromt$ java -jar ./target/ex_bestpractices-0.1-SNAPSHOT.jar
pkey: 10, x: 20
pkey: 1, x: 2
pkey: 100, x: 0
pkey: 10000, x: 20000
pkey: 1000, x: 2000
cmdpromt$
```
