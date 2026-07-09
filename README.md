# eventMesh

EventMesh is a generic event-driven infrastructure platform designed for modern microservices architectures. It provides a centralized event streaming layer powered by Apache Kafka, enabling independent services to communicate asynchronously without direct dependencies.

Unlike domain-specific applications, EventMesh is domain-agnostic and can be integrated into systems across e-commerce, healthcare, banking, logistics, IoT, fintech, and other event-driven applications. It acts as the communication backbone that allows services to publish and consume events reliably while providing complete operational visibility into the messaging infrastructure.

The platform focuses on reliability, observability, and scalability by offering tools to monitor event flow, consumer performance, message latency, and failure handling. Kafka topics act as communication channels between producers and consumers, while TimescaleDB stores time-series metrics for real-time monitoring and historical analysis.

Core Features
Event Streaming

Reliable asynchronous communication between microservices using Apache Kafka, enabling loosely coupled and highly scalable applications.

Topic Management

Create and manage Kafka topics with configurable partitions and replication factors to support varying throughput and fault tolerance requirements.

Dead Letter Queue (DLQ)

Automatically isolate failed messages into dedicated DLQ topics, making debugging, auditing, and recovery significantly easier.

Consumer Lag Monitoring

Continuously monitor consumer offsets and identify slow, stalled, or unhealthy consumer groups before they impact the system.

End-to-End Latency Tracking

Measure the complete lifecycle of an event—from production to successful consumption—to identify processing bottlenecks.

Replay Controller

Replay historical or failed events by resetting Kafka consumer offsets, allowing safe reprocessing without custom recovery scripts.

Service Dependency Graph

Visualize event flow between microservices to better understand service interactions and dependencies.

SLA Monitoring

Configure thresholds for latency and consumer lag, generating alerts whenever operational Service Level Agreements are violated.

Scalable Architecture

Supports multiple producers, consumers, consumer groups, and partition-based parallelism, allowing horizontal scaling with increasing workloads.

Technology Stack
Component	Technology
Event Streaming	Apache Kafka
Kafka Management	Kafka UI
Backend Services	Python
Metrics Storage	TimescaleDB (PostgreSQL)
Containerization	Docker & Docker Compose
Kafka Client	Confluent Kafka Python Client
