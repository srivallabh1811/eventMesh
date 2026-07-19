import { useMemo } from "react";
import ReactFlow, { Background, Controls, Position } from "reactflow";
import "reactflow/dist/style.css";

const COLUMN_X = { producer: 50, topic: 350, consumer: 650 };

const NODE_STYLE = {
  producer: {
    background: "var(--node-producer-bg)",
    color: "var(--node-producer-text)",
    border: "1px solid var(--node-producer-border)",
  },
  topic: {
    background: "var(--node-topic-bg)",
    color: "var(--node-topic-text)",
    border: "1px solid var(--node-topic-border)",
  },
  consumer: {
    background: "var(--node-consumer-bg)",
    color: "var(--node-consumer-text)",
    border: "1px solid var(--node-consumer-border)",
  },
};

const AT_RISK_STYLE = {
  background: "#3a1d1d",
  color: "#ff8a80",
  border: "2px solid #ff5252",
  boxShadow: "0 0 0 rgba(255, 82, 82, 0.5)",
};

const PATH_STYLE = {
  background: "#3a2d1d",
  color: "#ffcc80",
  border: "2px solid #f5a623",
};

function nodeId(type, name) {
  return `${type}:${name}`;
}

function buildGraph(edgesData, cascadeData) {
  const producers = new Set();
  const topics = new Set();
  const consumers = new Set();

  edgesData.forEach((e) => {
    producers.add(e.producer_client);
    topics.add(e.topic);
    consumers.add(e.consumer_group);
  });

  const atRiskConsumers = new Set();
  const pathNodeIds = new Set();
  const pathEdgeIds = new Set();
  const identityLinks = [];

  (cascadeData || []).forEach((prediction) => {
    const atRiskId = nodeId("consumer", prediction.at_risk_consumer_group);
    atRiskConsumers.add(atRiskId);

    prediction.downstream_impact.forEach((impact) => {
      const topicId = nodeId("topic", impact.via_topic);
      const consumerId = nodeId("consumer", impact.affected_consumer_group);

      const producingEdge = edgesData.find((e) => e.topic === impact.via_topic);
      if (producingEdge) {
        const producerId = nodeId("producer", producingEdge.producer_client);
        pathNodeIds.add(producerId);
        pathEdgeIds.add(`${producerId}->${topicId}`);

        if (impact.hops === 1) {
          identityLinks.push({ from: atRiskId, to: producerId });
        }
      }

      pathNodeIds.add(topicId);
      pathNodeIds.add(consumerId);
      pathEdgeIds.add(`${topicId}->${consumerId}`);
    });
  });

  const nodes = [];

  function addColumn(items, type) {
    Array.from(items).forEach((name, i) => {
      const id = nodeId(type, name);
      let style = { ...NODE_STYLE[type] };
      let className = "topology-node";

      if (atRiskConsumers.has(id)) {
        style = { ...style, ...AT_RISK_STYLE };
        className += " node-at-risk";
      } else if (pathNodeIds.has(id)) {
        style = { ...style, ...PATH_STYLE };
        className += " node-in-path";
      }

      nodes.push({
        id,
        position: { x: COLUMN_X[type], y: i * 80 + 20 },
        data: { label: name },
        className,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        style: {
          ...style,
          borderRadius: 8,
          padding: "8px 12px",
          fontSize: 13,
          width: 220,
        },
      });
    });
  }

  addColumn(producers, "producer");
  addColumn(topics, "topic");
  addColumn(consumers, "consumer");

  const edges = [];
  const seen = new Set();

  edgesData.forEach((e) => {
    const p2t = `${nodeId("producer", e.producer_client)}->${nodeId("topic", e.topic)}`;
    if (!seen.has(p2t)) {
      seen.add(p2t);
      const isPath = pathEdgeIds.has(p2t);
      edges.push({
        id: p2t,
        source: nodeId("producer", e.producer_client),
        target: nodeId("topic", e.topic),
        animated: isPath,
        style: isPath
          ? { stroke: "#f5a623", strokeWidth: 3 }
          : { stroke: "#5a6577" },
      });
    }

    const t2c = `${nodeId("topic", e.topic)}->${nodeId("consumer", e.consumer_group)}`;
    if (!seen.has(t2c)) {
      seen.add(t2c);
      const isPath = pathEdgeIds.has(t2c);
      edges.push({
        id: t2c,
        source: nodeId("topic", e.topic),
        target: nodeId("consumer", e.consumer_group),
        animated: isPath,
        style: isPath
          ? { stroke: "#f5a623", strokeWidth: 3 }
          : { stroke: "#5a6577" },
      });
    }
  });

  identityLinks.forEach((link, i) => {
    edges.push({
      id: `identity-${i}-${link.from}-${link.to}`,
      source: link.from,
      target: link.to,
      label: "same service",
      labelStyle: { fill: "#ff8a80", fontSize: 10 },
      style: { stroke: "#ff5252", strokeDasharray: "6 4", strokeWidth: 2 },
      animated: false,
    });
  });

  return { nodes, edges };
}

function TopologyGraph({ data, cascadeData }) {
  const { nodes, edges } = useMemo(
    () => buildGraph(data || [], cascadeData || []),
    [data, cascadeData]
  );

  if (!data || data.length === 0) {
    return <p className="empty-state">No topology data available.</p>;
  }

  const hasRisk = cascadeData && cascadeData.length > 0;

  return (
    <div>
      <div style={{ height: 420 }}>
        <ReactFlow nodes={nodes} edges={edges} fitView>
          <Background color="#262b38" gap={16} />
          <Controls />
        </ReactFlow>
      </div>
      <div className="graph-legend">
        <span><span className="legend-dot legend-dot-risk"></span> At risk (pulsing)</span>
        <span><span className="legend-dot" style={{ background: "#f5a623" }}></span> In cascade path</span>
        <span><span className="legend-dash"></span> Same physical service</span>
        {!hasRisk && <span className="legend-ok">Everything healthy right now</span>}
      </div>
    </div>
  );
}

export default TopologyGraph;