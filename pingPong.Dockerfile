FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :pingPong:distZip

RUN unzip pingPong/build/distributions/pingPong.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/pingPong /opt/pingPong

ENTRYPOINT ["java", "-cp", "/opt/pingPong/lib/*", "ac.at.uibk.dps.dapr.pingPong.PingPongKt"]
