FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :big:distZip

RUN unzip big/build/distributions/big.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/big /opt/big

ENTRYPOINT ["java", "-cp", "/opt/big/lib/*", "ac.at.uibk.dps.dapr.big.BigKt"]