const STATUS_COLORS = {
  pending: '#757575',
  assigned: '#1565c0',
  running: '#f9a825',
  completed: '#2e7d32',
  failed: '#c62828',
  reassigned: '#6a1b9a',
}

export default function JobList({ jobs, error, onSelectJob, selectedJobId }) {
  if (error) {
    return <p className="error">Couldn't reach the coordinator: {error}</p>
  }

  return (
    <section>
      <h2>Jobs</h2>
      {jobs.length === 0 ? (
        <p className="empty">No jobs submitted yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Command</th>
              <th>Status</th>
              <th>Worker</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr
                key={job.job_id}
                className={job.job_id === selectedJobId ? 'selected' : ''}
                onClick={() => onSelectJob(job.job_id)}
              >
                <td className="command">{job.command}</td>
                <td>
                  <span
                    className="status-dot"
                    style={{ backgroundColor: STATUS_COLORS[job.status] || '#999' }}
                  />
                  {job.status}
                </td>
                <td>{job.assigned_to || '—'}</td>
                <td>{job.created_at ? new Date(job.created_at).toLocaleTimeString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
