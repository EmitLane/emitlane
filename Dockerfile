# syntax=docker/dockerfile:1

FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/emitlane ./cmd/emitlane

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/emitlane /emitlane
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/emitlane"]
CMD ["run"]
