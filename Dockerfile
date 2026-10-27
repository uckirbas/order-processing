# syntax=docker/dockerfile:1
# One recipe for every service: PROJECT selects the project to publish, ASSEMBLY its entry assembly.
# Used by docker-compose.yml (profile "app"); see README, "Yük testi".

FROM mcr.microsoft.com/dotnet/sdk:10.0.402 AS build
ARG PROJECT
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props .editorconfig ./
COPY src/ src/
# NuGet cache mount: downloaded packages survive code changes without a separate restore layer. Locked, because
# compose builds the four images in parallel and concurrent extraction into one cache collides.
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages,sharing=locked \
    dotnet publish "$PROJECT" -c Release -o /app -p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0.13
ARG ASSEMBLY
ENV ASSEMBLY=$ASSEMBLY
WORKDIR /app
COPY --from=build /app .
# Non-root user shipped with the official images.
USER $APP_UID
# exec: dotnet becomes PID 1 and receives SIGTERM, so consumers drain and relays finish their batch on shutdown.
ENTRYPOINT ["sh", "-c", "exec dotnet \"$ASSEMBLY.dll\""]
