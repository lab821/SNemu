#base image
FROM registry.cn-beijing.aliyuncs.com/phantomu/docker-image:emuv2

# MAINTAINER
MAINTAINER fdjwd@126.com

# put varys into the container
ADD coflowemu /varys/

# RUN apt-get update && apt-get install -y net-tools

#RUN /coflowemu/sbt/sbt package

# add SCALA_HOME in env
ENV SCALA_HOME="/usr/local"

# set workdir
WORKDIR /varys/

# don't exit
CMD ["/bin/bash"]
