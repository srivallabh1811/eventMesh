# eventMesh

EventMesh is a generic event-driven infrastructure platform designed for modern microservices architectures. It provides a centralized event streaming layer using Apache Kafka, enabling independent services to communicate asynchronously without direct dependencies. The platform is domain-agnostic and can be integrated into applications across e-commerce, healthcare, banking, logistics, IoT, and any system built on microservices.

The project focuses on operational visibility and reliability by providing tools to monitor event flow, consumer performance, message latency, and failure handling. Kafka topics are used as communication channels between producers and consumers, while TimescaleDB stores time-series metrics for real-time monitoring and historical analysis.

Key Features
Event Streaming: Reliable asynchronous communication between microservices using Apache Kafka.
Topic Management: Create and manage Kafka topics with configurable partitions and replication.
Dead Letter Queue (DLQ) Support: Isolate failed messages for debugging and reprocessing.
Consumer Lag Monitoring: Track consumer offsets and identify slow or stalled consumers.
End-to-End Latency Tracking: Measure the time taken for events to travel from producer to consumer.
Replay Controller: Reprocess failed or historical events using Kafka offsets.
Service Dependency Graph: Visualize communication between microservices.
SLA Monitoring: Define latency and consumer lag thresholds for operational health monitoring.
Scalable Architecture: Supports multiple producers, consumers, and event-driven services with partition-based parallelism.
Technology Stack
Apache Kafka – Distributed event streaming platform
Kafka UI – Web-based Kafka management interface
Python – Producer, consumer, and backend services
TimescaleDB (PostgreSQL) – Time-series metrics storage
Docker & Docker Compose – Containerized deployment
Confluent Kafka Python Client – Kafka producer and consumer implementation
Architecture Overview
                Producer Services
                       │
                       ▼
               Apache Kafka Broker
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
  Consumer A      Consumer B      Consumer C
      │                │                │
      └────────────────┼────────────────┘
                       ▼
                TimescaleDB Metrics
                       │
                       ▼
          Monitoring & Analytics Dashboard

EventMesh serves as the communication backbone for distributed applications, allowing services to publish and consume events independently while providing comprehensive observability into the health and performance of the messaging infrastructure.
