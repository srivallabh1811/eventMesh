function LagTable({ data }) {
  if (!data || data.length === 0) {
  return <p className="empty-state">No lag data available.</p>;
}
  return (
    <table>
      <thead>
        <tr>
          <th>Consumer Group</th>
          <th>Topic</th>
          <th>Partition</th>
          <th>Lag</th>
          <th>Last Updated</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row) => (
          <tr key={`${row.consumer_group}-${row.topic}-${row.partition}`}>
            <td>{row.consumer_group}</td>
            <td>{row.topic}</td>
            <td>{row.partition}</td>
            <td>{row.lag}</td>
            <td>{new Date(row.time).toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default LagTable;