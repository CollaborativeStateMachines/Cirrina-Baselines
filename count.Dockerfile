FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :count:distZip

RUN unzip count/build/distributions/count.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/count /opt/count

ENTRYPOINT ["java", "-cp", "/opt/count/lib/*", "ac.at.uibk.dps.dapr.count.CountKt"]