function formatEta(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

function CascadeList({ data }) {
  if (!data || data.length === 0) {
    return <p className="empty-state">No cascade risks detected right now.</p>;
  }

  return (
    <div className="cascade-list">
      {data.map((prediction) => (
        <div key={`${prediction.at_risk_consumer_group}-${prediction.partition}`} className="cascade-item">
          <div className="cascade-item-header">
            <span className="trend-badge" style={{ backgroundColor: "var(--trend-worsening-bg)", color: "var(--trend-worsening-text)" }}>
              AT RISK
            </span>
            <strong>{prediction.at_risk_consumer_group}</strong>
            <span className="cascade-eta">breaching in {formatEta(prediction.eta_seconds)}</span>
          </div>
          {prediction.downstream_impact.length === 0 ? (
            <p className="cascade-no-impact">No downstream services currently depend on this one.</p>
          ) : (
            <ul className="cascade-impact-list">
              {prediction.downstream_impact.map((impact, i) => (
                <li key={i}>
                  via <code>{impact.via_topic}</code> affects <strong>{impact.affected_consumer_group}</strong>
                  {impact.hops > 1 ? ` (${impact.hops} hops away)` : ""}
                </li>
              ))}
            </ul>
          )}
        </div>
      ))}
    </div>
  );
}

export default CascadeList;