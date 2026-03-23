FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :diningPhilosophers:distZip

RUN unzip diningPhilosophers/build/distributions/diningPhilosophers.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/diningPhilosophers /opt/diningPhilosophers

ENTRYPOINT [ \
    "java", \
    "ac.at.uibk.dps.dapr.philosophers.DiningPhilosophersKt" \
]