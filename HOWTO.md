## Running Instructions For Eclipse

* Clone the repo
* Run:

```bash
$ mvn clean install eclipse:eclipse
```

#### Run From terminal

##### Node

* Run node
  * go to server/target/ 
  * Run *tar -xf ...-bin...tar.gz*
  * cd inside unpacked folder
  * *chmod +x run-node.sh*
  * run *./run-node*

##### Client

Same proccess but with client

#### Run From Eclipse

* Open Eclipse
  * Import as existing project

* Open run-configuration for client and set VM arguments:
  *-Dname=mt  -Dpass=dev-pass  -Daddresses=127.0.0.1*

* Move hazelcast from server/target/tpe-server-1.0-SNAPSHOT/ to server/

* Create run-configuration named *node* with main class: **com.hazelcast.console.ConsoleApp**

* Run node main

* Run client
