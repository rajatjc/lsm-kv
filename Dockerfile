FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /lsm-kv ./cmd/server

# Runs as root so it can write to platform-mounted volumes (Fly/Render mount them root-owned).
FROM gcr.io/distroless/static-debian12
COPY --from=build /lsm-kv /lsm-kv
ENV DATA_DIR=/data PORT=8080
EXPOSE 8080
ENTRYPOINT ["/lsm-kv"]
