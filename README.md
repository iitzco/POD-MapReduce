## Running Instructions For Eclipse

* Clone the repo
* Run:

```bash
$ mvn clean install eclipse:eclipse
```

#### Run From terminal

First, run *./make.sh* to generate scripts

##### Node

* cd to server/target/tpe-server-1.0-SNAPSHOT/
* run *./run-node*

**NOTE**: make sure to activate interfaces (false->true) and specify network in hazelcast.xml to activate multiple nodes in different computers. All computers need to have same hazelcast.xml


##### Client

* cd to client/target/tpe-client-1.0-SNAPSHOT/
* run *./run-client -Daddresses=xx.xx.xx.xx;yy.yy.yy.yy -Dquery=1 -DinPath=censo.csv -DoutPath=output.txt [queryParams]*

#### Run From Eclipse

* Open Eclipse
  * Import as existing project

* Open run-configuration for client and set VM arguments

* Move hazelcast from server/target/tpe-server-1.0-SNAPSHOT/ to server/

* Create run-configuration named *node* with main class: **com.hazelcast.console.ConsoleApp**

* Run node main

* Run client

**NOTE**: if after running *mvn clean install* or *./make.sh*, if want to work again from Eclipse, go to Project->Clean.
