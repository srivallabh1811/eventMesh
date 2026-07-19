const BASE_URL = "http://localhost:8000";

async function fetchJson(path) {
  const response = await fetch(`${BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`Request to ${path} failed: ${response.status}`);
  }
  return response.json();
}

export function getLag() {
  return fetchJson("/lag");
}

export function getVelocity() {
  return fetchJson("/velocity");
}

export function getTopology() {
  return fetchJson("/topology");
}

export function getThresholds() {
  return fetchJson("/thresholds");
}

export function getCascade() {
  return fetchJson("/cascade");
}

export function getDLQ() {
  return fetchJson("/dlq");
}