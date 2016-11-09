#!/bin/bash

java -cp 'lib/jars/*'  -Dname='mt'  -Dpass=dev-pass  -Daddresses='127.0.0.1'   "ar.edu.itba.pod.hz.client.VotacionClient" $*

