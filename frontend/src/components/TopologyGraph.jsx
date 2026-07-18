import { useMemo } from "react";
import ReactFlow, { Background, Controls } from "reactflow";
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
function nodeId(type, name) {
  return `${type}:${name}`;
}

function buildGraph(edgesData) {
  const producers = new Set();
  const topics = new Set();
  const consumers = new Set();

  edgesData.forEach((e) => {
    producers.add(e.producer_client);
    topics.add(e.topic);
    consumers.add(e.consumer_group);
  });

  const nodes = [];

  function addColumn(items, type) {
    Array.from(items).forEach((name, i) => {
      nodes.push({
        id: nodeId(type, name),
        position: { x: COLUMN_X[type], y: i * 80 + 20 },
        data: { label: name },
        style: {
          ...NODE_STYLE[type],
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
      edges.push({
        id: p2t,
        source: nodeId("producer", e.producer_client),
        target: nodeId("topic", e.topic),
        animated: true,
        style: { stroke: "#5a6577" },
      });
    }

    const t2c = `${nodeId("topic", e.topic)}->${nodeId("consumer", e.consumer_group)}`;
    if (!seen.has(t2c)) {
      seen.add(t2c);
      edges.push({
        id: t2c,
        source: nodeId("topic", e.topic),
        target: nodeId("consumer", e.consumer_group),
        animated: true,
        style: { stroke: "#5a6577" },
      });
    }
  });

  return { nodes, edges };
}

function TopologyGraph({ data }) {
  const { nodes, edges } = useMemo(() => buildGraph(data || []), [data]);

  if (!data || data.length === 0) {
    return <p className="empty-state">No topology data available.</p>;
  }

  return (
    <div style={{ height: 400 }}>
      <ReactFlow nodes={nodes} edges={edges} fitView>
        <Background color="#262b38" gap={16} />
        <Controls />
      </ReactFlow>
    </div>
  );
}

export default TopologyGraph;