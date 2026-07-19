// frontend/src/components/DLQTable.jsx

function formatSince(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString();
}

function severityStyle(count) {
  if (count >= 5) return { backgroundColor: "var(--trend-worsening-bg)", color: "var(--trend-worsening-text)" };
  if (count >= 2) return { backgroundColor: "var(--trend-stable-bg)", color: "var(--trend-stable-text)" };
  return { backgroundColor: "var(--trend-recovering-bg)", color: "var(--trend-recovering-text)" };
}

function DLQTable({ data }) {
  if (!data || data.length === 0) {
    return <p className="empty-state">No dead-lettered messages. Everything's flowing cleanly.</p>;
  }

  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Source</th>
            <th>Error Type</th>
            <th>Count</th>
            <th>Seen Since</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row) => (
            <tr key={`${row.source_group}-${row.error_type}`}>
              <td>{row.source_group}</td>
              <td>{row.error_type}</td>
              <td>
                <span className="trend-badge" style={severityStyle(row.count)}>
                  {row.count}
                </span>
              </td>
              <td>{formatSince(row.oldest_ts)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default DLQTable;