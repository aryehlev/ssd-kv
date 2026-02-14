FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o ssdkv-operator .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/ssdkv-operator /
USER 65532:65532
ENTRYPOINT ["/ssdkv-operator"]
