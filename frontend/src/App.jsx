import { useEffect, useState } from "react";
import { getLag, getVelocity, getThresholds, getTopology, getCascade } from "./api";
import LagTable from "./components/LagTable";
import VelocityTable from "./components/VelocityTable";
import ThresholdsTable from "./components/ThresholdsTable";
import TopologyGraph from "./components/TopologyGraph";
import CascadeList from "./components/CascadeList";
import ThemeToggle from "./ThemeToggle";
import "./App.css";

const REFRESH_INTERVAL_MS = 5000;

function App() {
  const [lag, setLag] = useState([]);
  const [velocity, setVelocity] = useState([]);
  const [thresholds, setThresholds] = useState([]);
  const [topology, setTopology] = useState([]);
  const [cascade, setCascade] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    function fetchAll() {
      getLag()
        .then((res) => {
          if (Array.isArray(res)) setLag(res);
        })
        .catch((err) => {
          console.warn("Lag fetch failed, keeping previous data:", err.message);
          setError(err.message);
        });

      getVelocity()
        .then((res) => {
          if (Array.isArray(res)) setVelocity(res);
        })
        .catch((err) => {
          console.warn("Velocity fetch failed, keeping previous data:", err.message);
          setError(err.message);
        });

      getThresholds()
        .then((res) => {
          if (Array.isArray(res)) setThresholds(res);
        })
        .catch((err) => {
          console.warn("Thresholds fetch failed, keeping previous data:", err.message);
          setError(err.message);
        });

      getTopology()
        .then((res) => {
          if (res && Array.isArray(res.edges)) setTopology(res.edges);
        })
        .catch((err) => {
          console.warn("Topology fetch failed, keeping previous data:", err.message);
          setError(err.message);
        });

      getCascade()
        .then((res) => {
          if (Array.isArray(res)) setCascade(res);
        })
        .catch((err) => {
          console.warn("Cascade fetch failed, keeping previous data:", err.message);
          setError(err.message);
        });
    }

    fetchAll();
    const interval = setInterval(fetchAll, REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <div className="dashboard-header-text">
          <h1>EventMesh</h1>
          <p>Kafka observability at a glance</p>
        </div>
        <ThemeToggle />
      </div>

      {error && <div className="error-banner">Error: {error}</div>}

      <div className="card">
        <h2>Service Topology</h2>
        <TopologyGraph data={topology} cascadeData={cascade} />
      </div>

      <div className="card">
        <h2>Cascade Risk</h2>
        <CascadeList data={cascade} />
      </div>

      <div className="grid-row">
        <div className="card">
          <h2>Consumer Lag</h2>
          <LagTable data={lag} />
        </div>

        <div className="card">
          <h2>SLA Thresholds</h2>
          <ThresholdsTable data={thresholds} />
        </div>
      </div>

      <div className="card">
        <h2>Velocity &amp; Trend</h2>
        <VelocityTable data={velocity} />
      </div>
    </div>
  );
}

export default App;