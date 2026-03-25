FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :chameneos:distZip

RUN unzip chameneos/build/distributions/chameneos.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/chameneos /opt/chameneos

ENTRYPOINT ["java", "-cp", "/opt/chameneos/lib/*", "ac.at.uibk.dps.dapr.chameneos.ChameneosKt"]