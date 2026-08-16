// Thin fetch wrapper around the coordinator's REST API. No client-side
// framework (axios etc.) needed for two GET endpoints.

const COORDINATOR_URL = import.meta.env.VITE_COORDINATOR_URL || 'http://localhost:8000'

async function getJSON(path) {
  const res = await fetch(`${COORDINATOR_URL}${path}`)
  if (!res.ok) {
    throw new Error(`${path} -> ${res.status} ${res.statusText}`)
  }
  return res.json()
}

export function getWorkers() {
  return getJSON('/workers?limit=100')
}

export function getJobs() {
  return getJSON('/jobs?limit=100')
}

export function getJob(jobId) {
  return getJSON(`/jobs/${jobId}`)
}
