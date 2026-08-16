import { useEffect, useState } from 'react'
import { getWorkers, getJobs } from './api'
import ClusterHealth from './components/ClusterHealth'
import JobList from './components/JobList'
import JobOutput from './components/JobOutput'

// Matches the worker heartbeat interval (common/config.py's
// HEARTBEAT_INTERVAL) — no point polling faster than the data underneath
// actually changes.
const POLL_INTERVAL_MS = 5000

export default function App() {
  const [workers, setWorkers] = useState([])
  const [jobs, setJobs] = useState([])
  const [error, setError] = useState(null)
  const [selectedJobId, setSelectedJobId] = useState(null)

  useEffect(() => {
    let cancelled = false

    async function poll() {
      try {
        const [workersData, jobsData] = await Promise.all([getWorkers(), getJobs()])
        if (cancelled) return
        setWorkers(workersData)
        setJobs(jobsData)
        setError(null)
      } catch (err) {
        if (!cancelled) setError(err.message)
      }
    }

    poll()
    const id = setInterval(poll, POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  return (
    <>
      <h1>Task Scheduler Dashboard</h1>
      <div className="layout">
        <div>
          <ClusterHealth workers={workers} error={error} />
          <JobList
            jobs={jobs}
            error={error}
            onSelectJob={setSelectedJobId}
            selectedJobId={selectedJobId}
          />
        </div>
        <JobOutput jobId={selectedJobId} />
      </div>
    </>
  )
}
