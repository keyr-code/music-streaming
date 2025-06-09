# Docker Setup for Music Streaming Project

This directory contains Docker configuration files for setting up the infrastructure required for the music streaming project.

## Services

The `docker-compose.yaml` file in the parent directory sets up the following services:

### Kafka Ecosystem
- **Zookeeper**: Coordination service for Kafka
- **Broker**: Kafka message broker
- **Control Center**: Web UI for managing Kafka
- **Schema Registry**: Service for managing Avro schemas

### Storage
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Relational database
- **pgAdmin**: Web UI for PostgreSQL

### Monitoring
- **Grafana**: Visualization and monitoring

## Usage

From the parent directory, run:

```bash
docker-compose up -d
```

To stop all services:

```bash
docker-compose down
```

## Service URLs

- Kafka Control Center: http://localhost:9021
- MinIO Console: http://localhost:9001 (minioadmin)
- pgAdmin: http://localhost:8080 (login: admin@admin.com / root)
- Grafana: http://localhost:3000 (login: admin / admin)

## Volumes

The following persistent volumes are created:
- `minio_streaming_data`: MinIO data
- `postgres_data`: PostgreSQL data
- `grafana_data`: Grafana dashboards and configurations