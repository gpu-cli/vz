ARG FIXTURE_BASE
FROM ${FIXTURE_BASE} AS build
ARG FIXTURE_BASE
ARG FIXTURE_VARIANT
ARG FIXTURE_RUN
COPY tools.py input.txt intermediate-canary.txt /fixture/
RUN --network=none python3 /fixture/tools.py payload
FROM scratch AS output
COPY --from=build /out/payload.txt /payload.txt
