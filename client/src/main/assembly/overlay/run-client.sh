#!/bin/bash

java -cp 'lib/jars/*'  -Dname='52539-53891'  -Dpass=pass  -Daddresses='127.0.0.1'  $*  "ar.edu.itba.pod.hz.client.Client"

