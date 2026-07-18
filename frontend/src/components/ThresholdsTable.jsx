function ThresholdsTable({ data }) {
  if (!data || data.length === 0) {
    return <p className="empty-state">No thresholds configured.</p>;
  }

  return (
    <table>
      <thead>
        <tr>
          <th>Consumer Group</th>
          <th>Topic</th>
          <th>Warn Lag</th>
          <th>High Lag</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row) => (
          <tr key={`${row.consumer_group}-${row.topic}`}>
            <td>{row.consumer_group}</td>
            <td>{row.topic}</td>
            <td>{row.warn_lag}</td>
            <td>{row.high_lag}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default ThresholdsTable;