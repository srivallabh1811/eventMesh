function formatEta(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

function trendStyle(trend) {
  if (trend === "WORSENING")
    return { backgroundColor: "var(--trend-worsening-bg)", color: "var(--trend-worsening-text)" };
  if (trend === "RECOVERING")
    return { backgroundColor: "var(--trend-recovering-bg)", color: "var(--trend-recovering-text)" };
  return { backgroundColor: "var(--trend-stable-bg)", color: "var(--trend-stable-text)" };
}

function VelocityTable({ data }) {
  if (!data || data.length === 0) {
    return <p className="empty-state">No velocity data available.</p>;
  }

  return (
     <div className="table-scroll">
    <table>
      <thead>
        <tr>
          <th>Consumer Group</th>
          <th>Topic</th>
          <th>Partition</th>
          <th>Current Lag</th>
          <th>Velocity (msg/sec)</th>
          <th>Trend</th>
          <th>ETA to Breach</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row) => (
          <tr key={`${row.consumer_group}-${row.topic}-${row.partition}`}>
            <td>{row.consumer_group}</td>
            <td>{row.topic}</td>
            <td>{row.partition}</td>
            <td>{row.current_lag}</td>
            <td>{row.velocity.toFixed(4)}</td>
            <td>
              <span className="trend-badge" style={trendStyle(row.trend)}>
                {row.trend}
              </span>
            </td>
            <td>{formatEta(row.eta_seconds)}</td>
          </tr>
        ))}
      </tbody>
    </table>
    </div>
  );
}

export default VelocityTable;