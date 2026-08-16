const STATUS_COLORS = {
  healthy: '#2e7d32',
  suspected: '#f9a825',
  dead: '#c62828',
  recovered: '#2e7d32',
}

export default function ClusterHealth({ workers, error }) {
  if (error) {
    return <p className="error">Couldn't reach the coordinator: {error}</p>
  }

  return (
    <section>
      <h2>Cluster health</h2>
      {workers.length === 0 ? (
        <p className="empty">No workers registered yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Worker</th>
              <th>Status</th>
              <th>Address</th>
              <th>Last heartbeat</th>
            </tr>
          </thead>
          <tbody>
            {workers.map((w) => (
              <tr key={w.worker_id}>
                <td>{w.worker_id}</td>
                <td>
                  <span
                    className="status-dot"
                    style={{ backgroundColor: STATUS_COLORS[w.status] || '#999' }}
                  />
                  {w.status}
                </td>
                <td>{w.address}:{w.port}</td>
                <td>{w.last_heartbeat ? new Date(w.last_heartbeat).toLocaleTimeString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
