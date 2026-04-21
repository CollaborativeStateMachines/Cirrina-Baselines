FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :dynamicPhilosophers:distZip

RUN unzip dynamicPhilosophers/build/distributions/dynamicPhilosophers.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/dynamicPhilosophers /opt/dynamicPhilosophers

ENTRYPOINT ["java", "-cp", "/opt/dynamicPhilosophers/lib/*", "ac.at.uibk.dps.dapr.philosophers.DynamicPhilosophersKt"]