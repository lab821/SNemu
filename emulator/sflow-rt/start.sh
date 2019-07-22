#!/bin/sh

HOME=`dirname $0`
cd $HOME

RTMEM="${RTMEM:-200M}"
JAR="./lib/sflowrt.jar"
RTJVM="-Xms${RTMEM} -Xmx${RTMEM} -XX:+UseG1GC -XX:MaxGCPauseMillis=100 ${RTJVM}"

exec java ${RTJVM} ${RTAPP} ${RTPROP} $@ -jar ${JAR}

