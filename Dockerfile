# Copyright 2019 AppScale Systems, Inc
#
# SPDX-License-Identifier: Apache-2.0
#
FROM ubuntu:bionic

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    ACDP_LOGGING=--console \
    ACDP_LISTEN=0.0.0.0 \
    ACDP_DATASTORE_HOST=datastore-1 \
    ACDP_DATASTORE_PORT=8888

EXPOSE 3500

RUN apt --assume-yes update \
 && apt --assume-yes install openjdk-8-jdk-headless

WORKDIR /root

COPY build.gradle gradlew settings.gradle \
             appscale-cloud-datastore-proxy/
COPY gradle  appscale-cloud-datastore-proxy/gradle/
COPY src     appscale-cloud-datastore-proxy/src/

RUN cd /root/appscale-cloud-datastore-proxy \
 && find /root/appscale-cloud-datastore-proxy/ -type f \
 && ./gradlew assemble \
 && cd /opt \
 && tar xvf /root/appscale-cloud-datastore-proxy/build/distributions/appscale-cloud-datastore-proxy.tar \
 && rm -rf /root/.gradle /root/appscale-cloud-datastore-proxy/build /root/appscale-cloud-datastore-proxy/.gradle

WORKDIR /opt/appscale-cloud-datastore-proxy

ENTRYPOINT ./bin/appscale-cloud-datastore-proxy \
  ${ACDP_LOGGING} \
  --listen-address=${ACDP_LISTEN} \
  --datastore-host=${ACDP_DATASTORE_HOST} \
  --datastore-port=${ACDP_DATASTORE_PORT}
