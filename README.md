# eventMesh

A robust, event-driven infrastructure platform designed for modern distributed microservices architectures. EventMesh provides a centralized event streaming layer built on Apache Kafka, enabling seamless asynchronous communication between independent services with comprehensive observability and operational monitoring.

## 🎯 Overview

EventMesh acts as the communication backbone for event-driven systems, providing reliable message delivery, comprehensive monitoring, and operational insights. It simplifies the complexity of managing distributed event streams while ensuring data consistency, fault tolerance, and system performance.

The platform focuses on operational visibility and reliability by providing tools to monitor event flow, consumer performance, message latency, and failure handling. Kafka topics serve as the communication channel between producers and consumers, with TimescaleDB storing time-series metrics for analytics.

## ✨ Key Features

- **Event Streaming**: Reliable asynchronous communication between microservices using Apache Kafka
- **Topic Management**: Create and manage Kafka topics with configurable partitions and replication factors
- **Dead Letter Queue (DLQ) Support**: Isolate failed messages for debugging and reprocessing
- **Consumer Lag Monitoring**: Real-time tracking of consumer offsets to identify slow or stalled consumers
- **End-to-End Latency Tracking**: Measure event propagation time from producer to consumer
- **Replay Controller**: Reprocess failed or historical events using Kafka offset management
- **Service Dependency Graph**: Visualize inter-service communication patterns
- **SLA Monitoring**: Define and enforce latency and consumer lag thresholds for operational health
- **Scalable Architecture**: Horizontal scaling with multiple producers, consumers, and partition-based parallelism
- **Comprehensive Dashboards**: Monitor system health, metrics, and performance in real-time

## 🛠️ Technology Stack

| Component | Purpose |
|-----------|---------|
| **Apache Kafka** | Distributed event streaming platform |
| **Kafka UI** | Web-based Kafka management and monitoring |
| **Python** | Producer, consumer, and backend services |
| **TimescaleDB** | Time-series metrics and analytics storage |
| **PostgreSQL** | Relational data persistence |
| **Docker & Docker Compose** | Containerized deployment and orchestration |
| **Confluent Kafka Python Client** | Kafka producer and consumer implementation |

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Producer Services                          │
│          (Microservices generating events)                      │
└───────────────────────┬──────────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │   Apache Kafka Brokers        │
        │  (Event Streaming Layer)      │
        │  - Topic Management           │
        │  - Partition Distribution     │
        │  - Replication & Failover     │
        └───────────────────────────────┘
                        │
        ┌──��────────────┼───────────────┐
        ▼               ▼               ▼
    ┌────────┐    ┌────────┐     ┌────────┐
    │Consumer│    │Consumer│     │Consumer│
    │  A     │    │  B     │     │  C     │
    └───┬────┘    └───┬────┘     └───┬────┘
        │             │              │
        └─────────────┼──────────────┘
                      ▼
        ┌──────────────────────────┐
        │ Event Processing Layer   │
        │ - DLQ Processing         │
        │ - Event Transformation   │
        │ - Replay Controller      │
        └───────────┬──────────────┘
                    ▼
        ┌──────────────────────────┐
        │  TimescaleDB Metrics     │
        │ (Time-Series Storage)    │
        │ - Latency Metrics        │
        │ - Consumer Lag           │
        │ - Throughput Data        │
        └───────────┬──────────────┘
                    ▼
        ┌──────────────────────────┐
        │ Monitoring Dashboard     │
        │ & Analytics              │
        │ - Real-time Dashboards   │
        │ - SLA Monitoring         │
        │ - Alerting & Reporting   │
        └──────────────────────────┘
```

## 📋 System Components

### Kafka Brokers
- Central message bus for event distribution
- Handles partitioning and replication
- Ensures durability and fault tolerance

### Producer Services
- Generate and publish events to Kafka topics
- Support multiple event types and schemas
- Track event metadata for tracing

### Consumer Services
- Subscribe to relevant Kafka topics
- Process events asynchronously
- Report processing status and failures

### Dead Letter Queue (DLQ)
- Captures failed message processing attempts
- Enables debugging and root cause analysis
- Supports message replay after fixes

### Monitoring & Analytics
- TimescaleDB stores performance metrics
- Tracks consumer lag and end-to-end latency
- Provides operational dashboards and alerts

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Basic understanding of Apache Kafka concepts

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/srivallabh1811/eventMesh.git
   cd eventMesh
   ```

2. **Start services with Docker Compose**
   ```bash
   docker-compose up -d
   ```

3. **Access Kafka UI**
   - Navigate to `http://localhost:8080` (Kafka UI)

4. **Configure topics and consumers**
   - Create topics via Kafka UI or CLI
   - Deploy consumer and producer services

## 📊 Monitoring & Observability

EventMesh provides comprehensive monitoring through:

- **Consumer Lag Dashboard**: Real-time tracking of message processing delays
- **Latency Analysis**: Monitor end-to-end event propagation times
- **Topic Metrics**: Message throughput, partition distribution, and health
- **Service Dependencies**: Visualize which services consume which topics
- **SLA Violations**: Alert when latency or lag thresholds are exceeded

## 🔄 Event Replay & Recovery

The Replay Controller allows you to:
- Replay events from specific timestamps or offsets
- Reprocess failed messages after fixes
- Perform bulk data reprocessing
- Maintain event ordering during replay

## 🔐 Best Practices

- **Schema Management**: Use consistent event schemas across producers
- **Topic Naming**: Follow naming conventions (e.g., `service.entity.action`)
- **Partition Strategy**: Distribute load evenly across partitions using appropriate keys
- **Consumer Groups**: Use distinct consumer groups for different processing pipelines
- **Monitoring**: Define SLAs and set up alerts for critical metrics
- **DLQ Processing**: Regularly review and reprocess DLQ messages

## 📝 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

---

**EventMesh** - Building reliable, event-driven systems with confidence.