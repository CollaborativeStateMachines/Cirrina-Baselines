FROM gradle:9.3.0-jdk25 AS build

COPY --chown=gradle:gradle . /usr/src/cirrina-baselines

WORKDIR /usr/src/cirrina-baselines

RUN gradle :sleepingBarber:distZip

RUN unzip sleepingBarber/build/distributions/sleepingBarber.zip -d /tmp

FROM gcr.io/distroless/java25-debian13 AS runtime

COPY --from=build /tmp/sleepingBarber /opt/sleepingBarber

ENTRYPOINT ["java", "-cp", "/opt/sleepingBarber/lib/*", "ac.at.uibk.dps.dapr.barber.SleepingBarberKt"]
