# ==========================
# Stage 1 — Build Go program
# ==========================
# FROM golang:1.24 AS builder
FROM golang:1.24 AS builder


# Set working directory
WORKDIR /app

# Install git to clone the repository
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Clone the Go repository (replace with the real repo URL)
RUN git clone https://github.com/herrBez/baffo.git baffo

# Move into the cloned repo
WORKDIR /app/baffo

RUN uname -a


# Build the Go binary
# RUN go mod tidy && go build -o /go/bin/baffo .

RUN go mod tidy \
    && go build -o /go/bin/baffo ./cmd/baffo


# ==========================
# Stage 2 — Python runtime
# ==========================
FROM python:3.13-slim

WORKDIR /app

# Copy your Python script(s) and dependencies
COPY main.py .
COPY requirements.txt .

# Copy compiled Go binary from the builder stage
COPY --from=builder /go/bin/baffo /usr/local/bin/baffo

RUN chmod +x /usr/local/bin/baffo
ENV PATH="/usr/local/bin:${PATH}"


# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command
CMD ["python", "main.py"]