# This Dockerfile utilizes a multi-stage builds
ARG ALPINE_VERSION=3.10

FROM golang:1.13-alpine$ALPINE_VERSION AS builder

# Install necessary build tools
RUN apk --update add make bash curl git

# Switch workdir, otherwise we end up in /go (default)
WORKDIR /

# Copy everything into build container
COPY . .

# Build the application
RUN make build/linux

# Now in 2nd build stage
FROM library/alpine:$ALPINE_VERSION

# Necessary depedencies
RUN apk --update add bash curl ca-certificates && update-ca-certificates

# Install binary
COPY --from=builder /build/plumber-linux /plumber-linux

CMD ["/plumber-linux", "relay"]
