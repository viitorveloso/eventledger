# --- build ---
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /eventledger ./cmd/api

# --- runtime ---
FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=build /eventledger /app/eventledger
COPY migrations /app/migrations
EXPOSE 8080
ENTRYPOINT ["/app/eventledger"]
