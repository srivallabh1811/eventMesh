import { useEffect, useState } from "react";
import { getLag, getVelocity, getThresholds, getTopology } from "./api";
import LagTable from "./components/LagTable";
import VelocityTable from "./components/VelocityTable";
import ThresholdsTable from "./components/ThresholdsTable";
import TopologyGraph from "./components/TopologyGraph";
import ThemeToggle from "./ThemeToggle";
import "./App.css";

const REFRESH_INTERVAL_MS = 5000;

function App() {
  const [lag, setLag] = useState([]);
  const [velocity, setVelocity] = useState([]);
  const [thresholds, setThresholds] = useState([]);
  const [topology, setTopology] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    function fetchAll() {
      getLag().then(setLag).catch((err) => setError(err.message));
      getVelocity().then(setVelocity).catch((err) => setError(err.message));
      getThresholds().then(setThresholds).catch((err) => setError(err.message));
      getTopology()
        .then((res) => setTopology(res.edges))
        .catch((err) => setError(err.message));
    }

    fetchAll(); // run immediately on load
    const interval = setInterval(fetchAll, REFRESH_INTERVAL_MS);

    return () => clearInterval(interval); // cleanup on unmount
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
      <TopologyGraph data={topology} />
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