import { useEffect, useState } from 'react'
import { getJob } from '../api'

// Renamed from the original stub's filename (JopOutput.jsx, a typo) while
// rebuilding this from scratch.
export default function JobOutput({ jobId }) {
  const [job, setJob] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!jobId) {
      setJob(null)
      return
    }
    let cancelled = false
    setError(null)
    getJob(jobId)
      .then((data) => {
        if (!cancelled) setJob(data)
      })
      .catch((err) => {
        if (!cancelled) setError(err.message)
      })
    return () => {
      cancelled = true
    }
  }, [jobId])

  if (!jobId) {
    return <p className="empty">Select a job to see its output.</p>
  }
  if (error) {
    return <p className="error">Couldn't load job {jobId}: {error}</p>
  }
  if (!job) {
    return <p className="empty">Loading…</p>
  }

  return (
    <section>
      <h2>Job output</h2>
      <dl className="job-detail">
        <dt>Command</dt>
        <dd><code>{job.command}</code></dd>
        <dt>Status</dt>
        <dd>{job.status}</dd>
        <dt>Exit code</dt>
        <dd>{job.exit_code ?? '—'}</dd>
      </dl>
      <pre className="job-output">{job.output || '(no output yet)'}</pre>
    </section>
  )
}
