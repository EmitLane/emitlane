# syntax=docker/dockerfile:1

FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ecommerce ./examples/ecommerce

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ecommerce /ecommerce
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/ecommerce"]
