FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :cigaretteSmokers:distZip

RUN unzip cigaretteSmokers/build/distributions/cigaretteSmokers.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/cigaretteSmokers /opt/cigaretteSmokers

ENTRYPOINT ["java", "-cp", "/opt/cigaretteSmokers/lib/*", "ac.at.uibk.dps.dapr.cigarette.CigaretteSmokersKt"]