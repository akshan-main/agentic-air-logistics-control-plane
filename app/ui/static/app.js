// Agentic Air Logistics Control Plane - UI Application

const API_BASE = '';  // Same origin

// State
let selectedCaseId = null;
let currentPacket = null;
let selectedAirport = null;

// DOM Elements
const airportInput = document.getElementById('airport-input');
const airportIcao = document.getElementById('airport-icao');
const airportResults = document.getElementById('airport-results');
const btnIngest = document.getElementById('btn-ingest');
const btnCreateCase = document.getElementById('btn-create-case');
const btnRunAgent = document.getElementById('btn-run-agent');
const btnRefreshCases = document.getElementById('btn-refresh-cases');
const btnSeedOps = document.getElementById('btn-seed-ops');
const btnClearOps = document.getElementById('btn-clear-ops');
const btnRefreshOps = document.getElementById('btn-refresh-ops');
const statusMessage = document.getElementById('status-message');
const casesList = document.getElementById('cases-list');
const packetPanel = document.getElementById('packet-panel');
const opsStats = document.getElementById('ops-stats');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    console.log('=== Agentic Air Logistics Control Plane UI v4 loaded ===');
    loadCases();
    loadOpsStats();
    setupEventListeners();
    updateButtonStates();
});

function setupEventListeners() {
    // Airport search
    airportInput.addEventListener('input', handleAirportSearch);
    airportInput.addEventListener('focus', handleAirportSearch);
    airportInput.addEventListener('keydown', handleAirportKeydown);
    document.addEventListener('click', (e) => {
        if (!e.target.closest('.airport-search')) {
            airportResults.classList.add('hidden');
        }
    });

    // Buttons
    btnIngest.addEventListener('click', handleIngest);
    btnCreateCase.addEventListener('click', handleCreateCase);
    btnRunAgent.addEventListener('click', handleRunAgent);
    btnRefreshCases.addEventListener('click', loadCases);

    if (btnSeedOps) btnSeedOps.addEventListener('click', handleSeedOps);
    if (btnClearOps) btnClearOps.addEventListener('click', handleClearOps);
    if (btnRefreshOps) btnRefreshOps.addEventListener('click', loadOpsStats);
}

// Airport Search Functionality
function handleAirportSearch() {
    const query = airportInput.value.trim();

    if (query.length < 2) {
        airportResults.classList.add('hidden');
        return;
    }

    const results = searchAirports(query);

    if (results.length === 0) {
        airportResults.innerHTML = '<div class="airport-result-item"><span class="name">No airports found</span></div>';
        airportResults.classList.remove('hidden');
        return;
    }

    airportResults.innerHTML = results.map((apt, index) => `
        <div class="airport-result-item" data-icao="${apt.icao}" data-index="${index}">
            <div><span class="icao">${apt.iata}</span> <span class="name">${apt.display.replace(apt.iata + ' - ', '')}</span></div>
            <div class="city">${apt.subtitle}</div>
        </div>
    `).join('');

    // Add click handlers
    airportResults.querySelectorAll('.airport-result-item[data-icao]').forEach(item => {
        item.addEventListener('click', () => selectAirport(item.dataset.icao));
    });

    airportResults.classList.remove('hidden');
}

function handleAirportKeydown(e) {
    const items = airportResults.querySelectorAll('.airport-result-item[data-icao]');
    const selectedItem = airportResults.querySelector('.airport-result-item.selected');
    let currentIndex = selectedItem ? parseInt(selectedItem.dataset.index) : -1;

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        currentIndex = Math.min(currentIndex + 1, items.length - 1);
        updateSelectedResult(items, currentIndex);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        currentIndex = Math.max(currentIndex - 1, 0);
        updateSelectedResult(items, currentIndex);
    } else if (e.key === 'Enter') {
        e.preventDefault();
        if (selectedItem) {
            selectAirport(selectedItem.dataset.icao);
        } else if (items.length > 0) {
            selectAirport(items[0].dataset.icao);
        }
    } else if (e.key === 'Escape') {
        airportResults.classList.add('hidden');
    }
}

function updateSelectedResult(items, index) {
    items.forEach((item, i) => {
        item.classList.toggle('selected', i === index);
    });
    if (items[index]) {
        items[index].scrollIntoView({ block: 'nearest' });
    }
}

function selectAirport(icao) {
    const airport = getAirportByIcao(icao);
    if (!airport) return;

    selectedAirport = airport;
    airportIcao.value = icao;  // Store ICAO for API calls
    airportInput.value = `${airport.iata} - ${airport.name}`;  // Show IATA to user
    airportResults.classList.add('hidden');

    // Show selected badge
    showSelectedAirport(airport);
    updateButtonStates();
}

function showSelectedAirport(airport) {
    // Remove existing badge if any
    const existing = document.querySelector('.airport-selected');
    if (existing) existing.remove();

    const badge = document.createElement('div');
    badge.className = 'airport-selected';
    badge.innerHTML = `
        <span class="icao-badge">${airport.iata}</span>
        <span>${airport.city}, ${airport.state}</span>
        <button class="clear-btn" title="Clear">&times;</button>
    `;

    badge.querySelector('.clear-btn').addEventListener('click', clearAirport);
    airportInput.parentElement.appendChild(badge);
}

function clearAirport() {
    selectedAirport = null;
    airportIcao.value = '';
    airportInput.value = '';

    const badge = document.querySelector('.airport-selected');
    if (badge) badge.remove();

    updateButtonStates();
    airportInput.focus();
}

function updateButtonStates() {
    const hasAirport = selectedAirport !== null;
    btnIngest.disabled = !hasAirport;
    btnCreateCase.disabled = !hasAirport;
    btnRunAgent.disabled = !selectedCaseId;
    if (btnSeedOps) btnSeedOps.disabled = !hasAirport;
    if (btnClearOps) btnClearOps.disabled = !hasAirport;
}

async function loadOpsStats() {
    if (!opsStats) return;
    try {
        const stats = await apiCall('/simulation/graph/operational-stats');
        const airports = (stats.airports_with_operational_data || []).join(', ') || '—';
        const totalNodes = stats.total_nodes ?? 0;
        const totalEdges = stats.total_edges ?? 0;
        opsStats.innerHTML = `
            Ops graph: <strong>${stats.has_operational_data ? 'seeded' : 'not seeded'}</strong><br>
            Airports: <span style="font-family: var(--font-mono);">${escapeHtml(airports)}</span><br>
            Nodes: ${totalNodes} · Edges: ${totalEdges}
        `;
    } catch (e) {
        opsStats.textContent = `Ops graph: unavailable (${e.message || e})`;
    }
}

function showStatus(message, type = 'info') {
    console.log(`Status [${type}]: ${message}`);
    statusMessage.textContent = message;
    statusMessage.className = `status-message ${type}`;
    statusMessage.classList.remove('hidden');

    if (type !== 'error') {
        setTimeout(() => {
            statusMessage.classList.add('hidden');
        }, 10000);
    }
}

async function apiCall(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: {
            'Content-Type': 'application/json',
        },
    };

    if (body) {
        options.body = JSON.stringify(body);
    }

    const response = await fetch(`${API_BASE}${endpoint}`, options);

    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(error.detail || 'API error');
    }

    return response.json();
}

// Handlers
async function handleIngest() {
    if (!selectedAirport) {
        showStatus('Please select an airport first', 'error');
        return;
    }

    const airport = selectedAirport.icao;
    btnIngest.disabled = true;
    btnIngest.textContent = 'Ingesting...';
    showStatus(`Ingesting signals for ${airport}... (this may take 10-20 seconds)`, 'info');

    try {
        const result = await apiCall(`/ingest/airport/${airport}`, 'POST', {});

        const successCount = result.sources_succeeded?.length || 0;
        const failedCount = result.sources_failed?.length || 0;
        const failedSources = result.sources_failed?.join(', ') || '';

        // Build detailed message
        let message = `Ingested ${airport}: ${successCount} sources OK`;

        if (failedCount > 0) {
            message += `, ${failedCount} failed (${failedSources})`;

            // Show specific errors if available
            if (result.errors?.length > 0) {
                const errorDetails = result.errors.map(e =>
                    `${e.source}: ${e.error?.substring(0, 50)}${e.error?.length > 50 ? '...' : ''}`
                ).join('; ');
                console.log('Ingestion errors:', result.errors);
                message += ` - ${errorDetails}`;
            }
        }

        showStatus(message, failedCount > 0 ? 'warning' : 'success');
    } catch (error) {
        console.error('Ingest error:', error);
        showStatus(`Ingest failed: ${error.message}`, 'error');
    } finally {
        btnIngest.disabled = false;
        btnIngest.textContent = 'Ingest Signals';
    }
}

async function handleSeedOps() {
    if (!selectedAirport) {
        showStatus('Please select an airport first', 'error');
        return;
    }

    const airport = selectedAirport.icao;
    if (btnSeedOps) {
        btnSeedOps.disabled = true;
        btnSeedOps.textContent = 'Refreshing...';
    }
    showStatus(`Refreshing SIMULATION ops graph for ${airport} (clear + seed)...`, 'info');

    try {
        const result = await apiCall(`/simulation/seed/airport/${airport}?refresh=true`, 'POST');
        const nodes = result.nodes_created || {};
        const cleared = result.cleared || null;
        const clearedMsg = cleared
            ? ` Cleared edges=${cleared.edges_deleted || 0}, nodes=${cleared.nodes_deleted || 0}.`
            : '';
        showStatus(
            `Refreshed ops graph for ${airport} (seed=${result.seed_used}). ` +
            `Flights=${nodes.flights || 0}, Shipments=${nodes.shipments || 0}, Bookings=${nodes.bookings || 0}.` +
            clearedMsg,
            'success'
        );
        await loadOpsStats();
    } catch (e) {
        showStatus(`Operational seeding failed: ${e.message || e}`, 'error');
    } finally {
        if (btnSeedOps) {
            btnSeedOps.textContent = 'Refresh Ops Graph';
            btnSeedOps.disabled = false;
        }
        updateButtonStates();
    }
}

async function handleClearOps() {
    if (!selectedAirport) {
        showStatus('Please select an airport first', 'error');
        return;
    }

    const airport = selectedAirport.icao;
    if (btnClearOps) {
        btnClearOps.disabled = true;
        btnClearOps.textContent = 'Clearing...';
    }
    showStatus(`Clearing SIMULATION ops graph for ${airport}...`, 'info');

    try {
        const result = await apiCall(`/simulation/seed/airport/${airport}`, 'DELETE');
        showStatus(
            `Cleared ops graph for ${airport}. Edges deleted=${result.edges_deleted || 0}, nodes deleted=${result.nodes_deleted || 0}.`,
            'success'
        );
        await loadOpsStats();
    } catch (e) {
        showStatus(`Operational clear failed: ${e.message || e}`, 'error');
    } finally {
        if (btnClearOps) {
            btnClearOps.textContent = 'Clear Ops Graph';
            btnClearOps.disabled = false;
        }
        updateButtonStates();
    }
}

async function handleCreateCase() {
    if (!selectedAirport) return;

    const airport = selectedAirport.icao;
    btnCreateCase.disabled = true;
    btnCreateCase.textContent = 'Creating...';

    try {
        const result = await apiCall('/cases', 'POST', {
            case_type: 'AIRPORT_DISRUPTION',
            scope: { airport },
        });

        showStatus(`Case created: ${result.case_id.substring(0, 8)}...`, 'success');

        if (result.playbook_suggested) {
            showStatus(
                `Case created. Playbook suggested: ${result.playbook_suggested.substring(0, 8)}...`,
                'info'
            );
        }

        await loadCases();
        selectCase(result.case_id);
    } catch (error) {
        showStatus(`Create case failed: ${error.message}`, 'error');
    } finally {
        btnCreateCase.disabled = false;
        btnCreateCase.textContent = 'Create Case';
    }
}

async function handleRunAgent() {
    if (!selectedCaseId) return;

    btnRunAgent.disabled = true;
    btnRunAgent.textContent = 'Running...';

    // Show progress panel
    showProgressPanel();

    try {
        // Use streaming endpoint for real-time progress
        const eventSource = new EventSource(`/cases/${selectedCaseId}/run/stream`);

        eventSource.onmessage = async (event) => {
            const data = JSON.parse(event.data);

            if (data.error) {
                eventSource.close();
                showStatus(`Agent failed: ${data.error}`, 'error');
                hideProgressPanel();
                btnRunAgent.disabled = false;
                btnRunAgent.textContent = 'Run Agent';
                return;
            }

            // Update progress display
            updateProgress(data);

            if (data.event === 'completed') {
                eventSource.close();
                showStatus(
                    `Agent complete. State: ${data.final_state}, ` +
                    `Actions: ${data.actions_executed} executed, ${data.actions_proposed} proposed`,
                    data.status === 'BLOCKED' ? 'warning' : 'success'
                );

                hideProgressPanel();
                await loadCases();
                await loadPacket(selectedCaseId);
                btnRunAgent.disabled = false;
                btnRunAgent.textContent = 'Run Agent';
            }
        };

        eventSource.onerror = (error) => {
            eventSource.close();
            showStatus('Agent connection error', 'error');
            hideProgressPanel();
            btnRunAgent.disabled = false;
            btnRunAgent.textContent = 'Run Agent';
        };

    } catch (error) {
        showStatus(`Agent failed: ${error.message}`, 'error');
        hideProgressPanel();
        btnRunAgent.disabled = false;
        btnRunAgent.textContent = 'Run Agent';
    }
}

function showProgressPanel() {
    // Create or show progress panel
    let panel = document.getElementById('progress-panel');
    if (!panel) {
        panel = document.createElement('div');
        panel.id = 'progress-panel';
        panel.className = 'progress-panel';
        panel.innerHTML = `
            <div class="progress-header">
                <h3>Agent Progress</h3>
                <div class="progress-spinner"></div>
            </div>
            <div class="progress-steps" id="progress-steps"></div>
            <div class="progress-stats" id="progress-stats"></div>
        `;
        document.querySelector('.packet-panel').insertBefore(panel, document.querySelector('.packet-panel').firstChild);
    }
    panel.classList.remove('hidden');
    document.getElementById('progress-steps').innerHTML = '';
    document.getElementById('progress-stats').innerHTML = '';
}

function hideProgressPanel() {
    const panel = document.getElementById('progress-panel');
    if (panel) {
        panel.classList.add('hidden');
    }
}

function updateProgress(data) {
    const stepsEl = document.getElementById('progress-steps');
    const statsEl = document.getElementById('progress-stats');

    if (!stepsEl || !statsEl) return;

    if (data.event === 'state_transition') {
        // Add step to progress with description
        const step = document.createElement('div');
        step.className = 'progress-step';

        // Color based on state
        let stateColor = '#58a6ff';
        if (data.to_state === 'INVESTIGATE') stateColor = '#f0ad4e';
        if (data.to_state === 'QUANTIFY_RISK') stateColor = '#dc3545';
        if (data.to_state === 'CRITIQUE') stateColor = '#17a2b8';
        if (data.to_state === 'EXECUTE') stateColor = '#6f42c1';
        if (data.to_state === 'COMPLETE') stateColor = '#28a745';

        step.innerHTML = `
            <div style="display: flex; align-items: flex-start; gap: 8px;">
                <span class="step-arrow" style="color: ${stateColor};">→</span>
                <div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span class="step-state" style="color: ${stateColor}; font-weight: 600;">${data.to_state}</span>
                        <span class="step-handler" style="color: #888; font-size: 0.75rem;">${data.handler}</span>
                    </div>
                    ${data.description ? `<div style="color: #ccc; font-size: 0.8rem; margin-top: 2px;">${escapeHtml(data.description)}</div>` : ''}
                    ${data.condition ? `<div style="color: #888; font-size: 0.7rem; margin-top: 2px;">When: ${data.condition}</div>` : ''}
                </div>
            </div>
        `;
        stepsEl.appendChild(step);
        stepsEl.scrollTop = stepsEl.scrollHeight;
    }

    if (data.event === 'progress') {
        // Update stats with description
        statsEl.innerHTML = `
            <div class="stat-item" style="grid-column: 1 / -1; margin-bottom: 8px;">
                <span class="stat-value" style="font-size: 0.85rem; color: #e0e0e0;">${data.description || data.state}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">State:</span>
                <span class="stat-value">${data.state}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Evidence:</span>
                <span class="stat-value">${data.evidence_count || 0}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Claims:</span>
                <span class="stat-value">${data.claim_count || 0}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Uncertainties:</span>
                <span class="stat-value">${data.uncertainty_count || 0}</span>
            </div>
            ${data.risk_level ? `
            <div class="stat-item">
                <span class="stat-label">Risk Level:</span>
                <span class="stat-value" style="color: ${data.risk_level === 'HIGH' ? '#dc3545' : data.risk_level === 'MEDIUM' ? '#f0ad4e' : '#28a745'};">${data.risk_level}</span>
            </div>
            ` : ''}
            ${data.recommended_posture ? `
            <div class="stat-item">
                <span class="stat-label">Posture:</span>
                <span class="stat-value posture-${data.recommended_posture}">${data.recommended_posture}</span>
            </div>
            ` : ''}
            ${data.confidence ? `
            <div class="stat-item">
                <span class="stat-label">Confidence:</span>
                <span class="stat-value">${(data.confidence * 100).toFixed(0)}%</span>
            </div>
            ` : ''}
        `;
    }

    if (data.event === 'started') {
        stepsEl.innerHTML = `
            <div class="progress-step">
                <span class="step-arrow" style="color: #28a745;">●</span>
                <span class="step-state">Started</span>
                <span style="color: #888; font-size: 0.8rem; margin-left: 8px;">Initializing agent orchestration</span>
            </div>
        `;
    }

    if (data.event === 'completed') {
        const step = document.createElement('div');
        step.className = 'progress-step completed';
        step.innerHTML = `
            <span class="step-arrow" style="color: #28a745;">✓</span>
            <span class="step-state" style="color: #28a745; font-weight: 600;">COMPLETE</span>
            <span class="step-handler">${data.status}</span>
        `;
        stepsEl.appendChild(step);
    }
}

async function loadCases() {
    try {
        const result = await apiCall('/cases?limit=20');
        renderCasesList(result.cases || []);
    } catch (error) {
        casesList.innerHTML = '<p class="empty-state">Failed to load cases</p>';
    }
}

function renderCasesList(cases) {
    if (cases.length === 0) {
        casesList.innerHTML = '<p class="empty-state">No cases found</p>';
        return;
    }

    casesList.innerHTML = cases.map(c => `
        <div class="case-item ${c.case_id === selectedCaseId ? 'selected' : ''}"
             data-case-id="${c.case_id}"
             onclick="selectCase('${c.case_id}')">
            <div class="case-id" style="font-family: monospace; font-size: 0.75rem; word-break: break-all;">${c.case_id}</div>
            <div class="case-type">${c.case_type}</div>
            <div>
                <span class="case-status ${c.status}">${c.status}</span>
                ${c.scope?.airport ? `<span style="margin-left: 10px;">${c.scope.airport}</span>` : ''}
            </div>
        </div>
    `).join('');
}

async function selectCase(caseId) {
    selectedCaseId = caseId;
    updateButtonStates();

    // Update visual selection
    document.querySelectorAll('.case-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.caseId === caseId);
    });

    // Load packet if case is resolved
    await loadPacket(caseId);
}

async function loadPacket(caseId) {
    try {
        const packet = await apiCall(`/packets/${caseId}`);
        currentPacket = packet;
        renderPacket(packet);
        packetPanel.classList.remove('hidden');
    } catch (error) {
        // Case might not have a packet yet
        packetPanel.classList.add('hidden');
    }
}

function renderPacket(packet) {
    // Posture badge
    const postureBadge = document.getElementById('posture-badge');
    postureBadge.textContent = packet.posture || '--';
    postureBadge.className = `posture-badge ${packet.posture || ''}`;

    // Meta
    document.getElementById('packet-case-id').textContent = `Case: ${packet.case_id}`;
    document.getElementById('packet-pdl').textContent = `PDL: ${
        packet.metrics?.pdl_seconds != null
            ? `${packet.metrics.pdl_seconds.toFixed(1)}s`
            : '--'
    }`;

    // Scope
    document.getElementById('packet-scope').textContent = JSON.stringify(packet.scope, null, 2);

    // Timestamps
    const timestamps = packet.timestamps || {};
    document.getElementById('packet-timestamps').innerHTML = `
        <div>Created: ${formatTimestamp(timestamps.created_at)}</div>
        <div>First Signal: ${formatTimestamp(timestamps.first_signal_at)}</div>
        <div>Posture Emitted: ${formatTimestamp(timestamps.posture_emitted_at)}</div>
    `;

    // Claims
    document.getElementById('claims-count').textContent = packet.claims?.length || 0;
    document.getElementById('packet-claims').innerHTML = renderClaims(packet.claims);

    // Evidence
    document.getElementById('evidence-count').textContent = packet.evidence?.length || 0;
    document.getElementById('packet-evidence').innerHTML = renderEvidence(packet.evidence);

    // Contradictions
    document.getElementById('packet-contradictions').innerHTML = renderContradictions(packet.contradictions);

    // Actions
    document.getElementById('packet-actions').innerHTML = renderActions(
        packet.actions_proposed,
        packet.actions_executed
    );

    // Blocked section
    const blockedSection = document.getElementById('blocked-section');
    if (packet.blocked_section?.is_blocked) {
        blockedSection.classList.remove('hidden');
        document.getElementById('packet-blocked').innerHTML = renderBlocked(
            packet.blocked_section.missing_evidence_requests
        );
    } else {
        blockedSection.classList.add('hidden');
    }

    // Policies
    document.getElementById('packet-policies').innerHTML = renderPolicies(packet.policies_applied);

    // Workflow trace
    const workflowSection = document.getElementById('workflow-section');
    if (workflowSection) {
        document.getElementById('packet-workflow').innerHTML = renderWorkflowTrace(packet.workflow_trace);
    }

    // Confidence breakdown
    const confidenceSection = document.getElementById('confidence-section');
    if (confidenceSection && packet.confidence_breakdown) {
        document.getElementById('packet-confidence').innerHTML = renderConfidenceBreakdown(packet.confidence_breakdown);
    }

    // Cascade impact (operational data)
    const cascadeSection = document.getElementById('cascade-section');
    if (cascadeSection) {
        if (packet.cascade_impact) {
            cascadeSection.classList.remove('hidden');
            document.getElementById('packet-cascade').innerHTML = renderCascadeImpact(packet.cascade_impact);
        } else {
            cascadeSection.classList.add('hidden');
        }
    }
}

function formatTimestamp(ts) {
    if (!ts || ts === 'None') return '--';
    try {
        return new Date(ts).toLocaleString();
    } catch {
        return ts;
    }
}

function renderClaims(claims) {
    if (!claims?.length) return '<p class="empty-state">No claims</p>';

    return claims.map(c => `
        <div class="claim-item">
            <div class="claim-text">${escapeHtml(c.text)}</div>
            <div class="claim-meta">
                Status: ${c.status} | Confidence: ${(c.confidence * 100).toFixed(0)}%
            </div>
        </div>
    `).join('');
}

function renderEvidence(evidence) {
    if (!evidence?.length) return '<p class="empty-state">No evidence</p>';

    return evidence.map(e => {
        const parsed = parseEvidenceExcerpt(e.source_system, e.excerpt);
        return `
            <div class="evidence-item" style="padding: 12px; margin: 6px 0; background: rgba(255,255,255,0.03); border-radius: 6px; border-left: 3px solid ${parsed.color};">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                    <span style="font-weight: 600; color: ${parsed.color}; font-size: 0.9rem;">${parsed.icon} ${e.source_system}</span>
                    <span style="font-size: 0.75rem; color: #888;">${formatTimestamp(e.retrieved_at)}</span>
                </div>
                <div style="font-size: 0.85rem; line-height: 1.5;">${parsed.summary}</div>
                ${parsed.details.length > 0 ? `
                    <div style="display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px;">
                        ${parsed.details.map(d => `
                            <span style="font-size: 0.75rem; padding: 2px 8px; background: rgba(255,255,255,0.06); border-radius: 4px; color: #aaa;">
                                ${escapeHtml(d)}
                            </span>
                        `).join('')}
                    </div>
                ` : ''}
            </div>
        `;
    }).join('');
}

function parseEvidenceExcerpt(source, excerpt) {
    const result = { summary: '', details: [], color: '#58a6ff', icon: '' };
    if (!excerpt) return { ...result, summary: 'No data available' };

    let data;
    try {
        data = JSON.parse(excerpt.replace(/^"|"$/g, ''));
    } catch {
        // Not JSON — show as-is but trim intelligently
        return { ...result, summary: escapeHtml(excerpt.substring(0, 300)) };
    }

    // Handle arrays (e.g., NWS alerts)
    if (Array.isArray(data)) {
        if (data.length === 0) {
            return { summary: 'No active alerts (normal conditions)', details: [], color: '#28a745', icon: '' };
        }
        const items = data.slice(0, 3).map(alert => {
            const severity = alert.severity || 'Unknown';
            const sevColor = severity === 'Severe' ? '#dc3545' : severity === 'Moderate' ? '#ffc107' : '#28a745';
            return `<div style="margin-bottom: 6px; padding: 6px 8px; background: rgba(255,255,255,0.03); border-radius: 4px;">
                <span style="color: ${sevColor}; font-weight: 600;">${escapeHtml(alert.event || 'Alert')}</span>
                <span style="color: #888; margin-left: 8px; font-size: 0.8rem;">${severity} / ${alert.certainty || ''}</span>
                ${alert.headline ? `<div style="font-size: 0.8rem; color: #ccc; margin-top: 4px;">${escapeHtml(alert.headline.substring(0, 150))}</div>` : ''}
            </div>`;
        });
        const more = data.length > 3 ? `<div style="color: #888; font-size: 0.8rem;">+ ${data.length - 3} more alerts</div>` : '';
        return { summary: items.join('') + more, details: [], color: '#ffc107', icon: '' };
    }

    // Handle status-based evidence (api_error, no_disruption, normal ops)
    if (data.status) {
        switch (data.status) {
            case 'normal_operations':
            case 'no_disruption':
                result.icon = '';
                result.color = '#28a745';
                result.summary = escapeHtml(data.message || 'Normal operations — no disruptions');
                break;
            case 'no_data':
                result.icon = '';
                result.color = '#6c757d';
                result.summary = escapeHtml(data.message || 'No active data from this source');
                break;
            case 'api_error':
            case 'not_fetched':
                result.icon = '';
                result.color = '#dc3545';
                result.summary = escapeHtml(data.message || data.error || 'Failed to fetch from source');
                break;
        }
        return result;
    }

    // Source-specific parsing
    switch (source) {
        case 'FAA_NAS': {
            const hasDisruption = data.delay || data.closure || data.ground_stop;
            result.icon = '';
            result.color = hasDisruption ? '#dc3545' : '#28a745';
            if (hasDisruption) {
                result.summary = 'Disruption detected';
                if (data.delay) result.details.push(`Delay: ${data.delay}`);
                if (data.closure) result.details.push('Closure active');
                if (data.ground_stop) result.details.push('Ground stop');
                if (data.ground_delay) result.details.push(`Ground delay: ${data.ground_delay}`);
            } else {
                result.summary = 'No FAA disruptions — normal operations';
            }
            break;
        }
        case 'METAR': {
            result.icon = '';
            const vis = data.visibility_miles;
            const wind = data.wind_speed;
            const gust = data.wind_gust;
            const ceiling = data.ceiling_feet;
            const cat = data.flight_category;

            const isGood = (!vis || parseFloat(vis) >= 6) && (!wind || wind < 20);
            result.color = isGood ? '#28a745' : '#ffc107';

            result.summary = data.raw_text
                ? escapeHtml(data.raw_text)
                : `Conditions at ${data.icao || ''}`;

            if (vis) result.details.push(`Visibility: ${vis}mi`);
            if (wind != null) result.details.push(`Wind: ${wind}kt${gust ? ` (gusts ${gust}kt)` : ''}`);
            if (ceiling) result.details.push(`Ceiling: ${ceiling}ft ${data.ceiling_type || ''}`);
            if (data.temp_c != null) result.details.push(`Temp: ${data.temp_c}C`);
            if (cat) result.details.push(`Category: ${cat}`);
            break;
        }
        case 'TAF': {
            result.icon = '';
            result.color = '#17a2b8';
            result.summary = data.raw_text
                ? escapeHtml(data.raw_text)
                : `Forecast for ${data.icao || ''}`;
            if (data.valid_from) result.details.push(`Valid from: ${formatTimestamp(data.valid_from)}`);
            if (data.valid_to) result.details.push(`Valid to: ${formatTimestamp(data.valid_to)}`);
            break;
        }
        case 'NWS_ALERTS': {
            result.icon = '';
            result.color = '#ffc107';
            result.summary = 'See alerts above';
            break;
        }
        case 'OPENSKY': {
            result.icon = '';
            result.color = '#6f42c1';
            const states = data.states;
            if (Array.isArray(states)) {
                result.summary = `${states.length} aircraft tracked in vicinity`;
                result.details.push(`Snapshot time: ${data.time ? new Date(data.time * 1000).toLocaleTimeString() : 'N/A'}`);
            } else if (typeof states === 'number' || (typeof states === 'string' && !isNaN(states))) {
                result.summary = `${states} aircraft tracked`;
            } else {
                result.summary = 'Aircraft movement data captured';
            }
            break;
        }
        default:
            result.summary = escapeHtml(excerpt.substring(0, 200));
    }
    return result;
}

function renderContradictions(contradictions) {
    if (!contradictions?.length) return '<p class="empty-state">No contradictions detected</p>';

    return contradictions.map(c => `
        <div class="contradiction-item" style="padding: 10px; margin: 5px 0; background: rgba(255, 193, 7, 0.1); border-radius: 4px; border-left: 3px solid #ffc107;">
            <div style="font-weight: 600; color: #856404;">
                ${c.contradiction_type || 'Signal Mismatch'}
            </div>
            <div style="font-size: 0.85rem; margin-top: 4px;">
                Claim A: ${c.claim_a_id?.substring(0, 8) || 'N/A'}... vs Claim B: ${c.claim_b_id?.substring(0, 8) || 'N/A'}...
            </div>
            <div style="font-size: 0.85rem; margin-top: 4px;">
                Status: <span style="color: ${c.resolution_status === 'OPEN' ? '#dc3545' : '#28a745'}; font-weight: 500;">
                    ${c.resolution_status || 'OPEN'}
                </span>
            </div>
        </div>
    `).join('');
}

function renderActions(proposed, executed) {
    const all = [
        ...(proposed || []).map(a => ({ ...a, executed: false })),
    ];

    // Mark executed actions
    const executedIds = new Set((executed || []).map(e => e.action_id));
    all.forEach(a => {
        if (executedIds.has(a.action_id)) {
            a.executed = true;
        }
    });

    if (!all.length) return '<p class="empty-state">No actions</p>';

    return all.map(a => `
        <div class="action-item">
            <div>
                <span class="action-type">${a.type}</span>
                ${a.args ? `<span style="color: var(--color-text-muted); margin-left: 10px;">${JSON.stringify(a.args)}</span>` : ''}
            </div>
            <span class="action-state ${a.state}">${a.state}</span>
        </div>
    `).join('');
}

function renderBlocked(requests) {
    if (!requests?.length) return '<p class="empty-state">No blocking issues</p>';

    return requests.map(r => `
        <div class="blocked-item">
            <div class="blocked-source">${r.source_system} - ${r.request_type}</div>
            <div class="blocked-reason">${escapeHtml(r.reason)}</div>
            <div style="margin-top: 5px; font-size: 0.8rem;">
                Criticality: ${r.criticality}
            </div>
        </div>
    `).join('');
}

function renderPolicies(policies) {
    if (!policies?.length) return '<p class="empty-state">No policies applied</p>';

    return policies.map(p => {
        const isBlocked = p.effect?.includes('BLOCKED');
        const bgColor = isBlocked ? 'rgba(220, 53, 69, 0.1)' : 'rgba(40, 167, 69, 0.1)';
        const borderColor = isBlocked ? '#dc3545' : '#28a745';
        const effectColor = isBlocked ? '#dc3545' : '#28a745';

        return `
            <div class="policy-item" style="padding: 10px; margin: 5px 0; background: ${bgColor}; border-radius: 4px; border-left: 3px solid ${borderColor};">
                <div style="font-weight: 500;">${escapeHtml(p.policy_text)}</div>
                <div style="font-size: 0.85rem; margin-top: 4px;">
                    Effect: <span style="color: ${effectColor}; font-weight: 500;">${p.effect || 'APPLIED'}</span>
                </div>
            </div>
        `;
    }).join('');
}

function renderWorkflowTrace(trace) {
    if (!trace?.length) return '<p class="empty-state">No workflow trace available</p>';

    // Group by state transitions
    const stateEnters = trace.filter(t => t.event_type === 'STATE_ENTER');

    return `
        <div style="font-family: system-ui, -apple-system, sans-serif; font-size: 0.85rem; background: #1a1a2e; padding: 16px; border-radius: 8px;">
            ${stateEnters.map((t, i) => {
                const state = t.state || 'UNKNOWN';
                const meta = t.meta || {};
                const isLast = i === stateEnters.length - 1;
                const description = meta.description || '';

                // Determine color based on state
                let color = '#58a6ff';  // Default blue
                if (isLast) color = '#28a745';  // Green for final
                if (state === 'INVESTIGATE') color = '#f0ad4e';  // Orange for investigation
                if (state === 'QUANTIFY_RISK') color = '#dc3545';  // Red for risk
                if (state === 'CRITIQUE') color = '#17a2b8';  // Cyan for critique
                if (state === 'EXECUTE') color = '#6f42c1';  // Purple for execute

                // Build detail items from meta
                const details = [];
                if (meta.condition_met) details.push(`Condition: ${meta.condition_met}`);
                if (meta.risk_level) details.push(`Risk: ${meta.risk_level}`);
                if (meta.recommended_posture) details.push(`Posture: ${meta.recommended_posture}`);
                if (meta.confidence) details.push(`Confidence: ${Math.round(meta.confidence * 100)}%`);
                if (meta.critic_verdict) details.push(`Critic: ${meta.critic_verdict}`);
                if (meta.policy_verdict) details.push(`Policy: ${meta.policy_verdict}`);
                if (meta.actions_planned) details.push(`Actions: ${meta.actions_planned}`);
                if (meta.evidence_count !== undefined) details.push(`Evidence: ${meta.evidence_count}`);
                if (meta.uncertainty_count !== undefined) details.push(`Uncertainties: ${meta.uncertainty_count}`);

                return `
                    <div style="margin-bottom: 12px; padding-left: 24px; border-left: 2px solid ${color}; position: relative;">
                        <div style="position: absolute; left: -7px; top: 2px; width: 12px; height: 12px; border-radius: 50%; background: ${color};"></div>
                        <div style="display: flex; align-items: center; margin-bottom: 4px;">
                            <span style="color: ${color}; font-weight: 600; font-size: 0.9rem;">${state}</span>
                            ${meta.handler ? `<span style="color: #888; margin-left: 10px; font-size: 0.75rem; font-family: monospace;">→ ${meta.handler}</span>` : ''}
                        </div>
                        ${description ? `<div style="color: #e0e0e0; margin-bottom: 4px;">${escapeHtml(description)}</div>` : ''}
                        ${details.length > 0 ? `
                            <div style="display: flex; flex-wrap: wrap; gap: 6px; margin-top: 4px;">
                                ${details.map(d => `
                                    <span style="font-size: 0.75rem; padding: 2px 6px; background: rgba(255,255,255,0.1); border-radius: 3px; color: #aaa;">${d}</span>
                                `).join('')}
                            </div>
                        ` : ''}
                    </div>
                `;
            }).join('')}
        </div>
    `;
}

function renderConfidenceBreakdown(breakdown) {
    if (!breakdown) return '<p class="empty-state">No confidence breakdown available</p>';

    const sources = breakdown.sources || {};
    const penalties = breakdown.penalties || {};
    const explanation = breakdown.explanation || '';

    return `
        <div style="font-size: 0.9rem;">
            <div style="margin-bottom: 10px; padding: 10px; background: rgba(88, 166, 255, 0.1); border-radius: 6px;">
                <strong>Final Confidence:</strong> ${Math.round((breakdown.final || 0) * 100)}%
            </div>

            <div style="margin-bottom: 8px;"><strong>Data Sources:</strong></div>
            <div style="display: grid; gap: 6px; margin-bottom: 12px;">
                ${Object.entries(sources).map(([source, value]) => {
                    const isMissing = value === 'missing';
                    const color = isMissing ? '#dc3545' : '#28a745';
                    const icon = isMissing ? '✗' : '✓';
                    return `
                        <div style="display: flex; align-items: center; padding: 6px 10px; background: ${isMissing ? 'rgba(220, 53, 69, 0.1)' : 'rgba(40, 167, 69, 0.1)'}; border-radius: 4px;">
                            <span style="color: ${color}; margin-right: 8px; font-weight: bold;">${icon}</span>
                            <span style="font-weight: 500; min-width: 100px;">${source}</span>
                            <span style="color: ${isMissing ? '#dc3545' : '#666'}; font-size: 0.85rem;">${value}</span>
                        </div>
                    `;
                }).join('')}
            </div>

            ${Object.keys(penalties).length > 0 ? `
                <div style="margin-bottom: 8px;"><strong>Penalties:</strong></div>
                <div style="display: grid; gap: 6px; margin-bottom: 12px;">
                    ${Object.entries(penalties).map(([reason, value]) => `
                        <div style="padding: 6px 10px; background: rgba(255, 193, 7, 0.1); border-radius: 4px; color: #856404;">
                            ${reason}: ${value}
                        </div>
                    `).join('')}
                </div>
            ` : ''}

            ${explanation ? `
                <div style="padding: 10px; background: var(--color-bg); border-radius: 6px; font-size: 0.85rem; color: var(--color-text-muted);">
                    ${escapeHtml(explanation)}
                </div>
            ` : ''}
        </div>
    `;
}

function renderCascadeImpact(impact) {
    if (!impact) return '<p class="empty-state">No operational data available</p>';
    if (impact.error) return `<p class="empty-state">Error loading cascade data: ${escapeHtml(impact.error)}</p>`;

    const impactAirport = impact.airport || '';
    const summary = impact.summary || {};
    const flights = impact.flights || [];
    const shipments = impact.shipments || [];
    const sla_exposure = impact.sla_exposure || [];
    const carriers = impact.carriers || [];
    const claims = impact.claims || [];
    const evidence_sources = impact.evidence_sources || [];
    const network = impact.network_position || {};
    const graph_traversal = impact.graph_traversal || {};
    const edge_types = graph_traversal.edge_types || {};
    const operational = impact.operational_data || {};
    const operationalSources = operational.sources || [];
    const isOperationalSimulated = Boolean(operational.is_simulated);

    // Format SLA deadline for display
    const formatDeadline = (deadline) => {
        if (!deadline) return 'N/A';
        const d = new Date(deadline);
        const now = new Date();
        const hoursUntil = Math.round((d - now) / (1000 * 60 * 60));
        if (hoursUntil < 0) return `OVERDUE by ${Math.abs(hoursUntil)}h`;
        if (hoursUntil < 24) return `${hoursUntil}h remaining`;
        return `${Math.round(hoursUntil / 24)}d remaining`;
    };

    return `
        <div style="font-size: 0.9rem;">
            <!-- Business Context -->
            <div style="margin-bottom: 16px; padding: 10px 14px; background: rgba(88, 166, 255, 0.08); border-radius: 6px; font-size: 0.8rem; color: #aaa; line-height: 1.5;">
                <strong style="color: #58a6ff;">What is this?</strong>
                This is the downstream impact analysis for <strong style="color: #e0e0e0;">${escapeHtml(impactAirport)}</strong>.
                It shows all flights, shipments, bookings, and carriers that flow through this gateway.
                A disruption here cascades to these entities — the revenue figure is the forwarder's booking charges at risk (not cargo value).
                SLA deadlines approaching within 24h are flagged as imminent breaches.
                ${operationalSources.length ? `
                    <br><br>
                    <strong style="color: ${isOperationalSimulated ? '#ffc107' : '#58a6ff'};">Operational data source:</strong>
                    ${escapeHtml(operationalSources.join(', '))}${isOperationalSimulated ? ' (simulated demo data)' : ''}
                ` : ''}
            </div>

            <!-- Summary Stats -->
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 16px;">
                <div style="text-align: center; padding: 12px; background: rgba(255, 193, 7, 0.1); border-radius: 8px; border: 1px solid rgba(255, 193, 7, 0.3);">
                    <div style="font-size: 1.5rem; font-weight: 700; color: #ffc107;">${summary.total_flights || 0}</div>
                    <div style="font-size: 0.75rem; color: #aaa;">Flights Affected</div>
                </div>
                <div style="text-align: center; padding: 12px; background: rgba(220, 53, 69, 0.1); border-radius: 8px; border: 1px solid rgba(220, 53, 69, 0.3);">
                    <div style="font-size: 1.5rem; font-weight: 700; color: #dc3545;">${summary.total_shipments || 0}</div>
                    <div style="font-size: 0.75rem; color: #aaa;">Shipments Affected</div>
                </div>
                <div style="text-align: center; padding: 12px; background: rgba(40, 167, 69, 0.1); border-radius: 8px; border: 1px solid rgba(40, 167, 69, 0.3);">
                    <div style="font-size: 1.5rem; font-weight: 700; color: #28a745;">$${(summary.total_revenue_usd || 0).toLocaleString()}</div>
                    <div style="font-size: 0.75rem; color: #aaa;">Revenue Exposed</div>
                </div>
                <div style="text-align: center; padding: 12px; background: rgba(111, 66, 193, 0.1); border-radius: 8px; border: 1px solid rgba(111, 66, 193, 0.3);">
                    <div style="font-size: 1.5rem; font-weight: 700; color: #6f42c1;">${summary.sla_breaches_imminent || 0}</div>
                    <div style="font-size: 0.75rem; color: #aaa;">SLA Breaches (&lt;24h)</div>
                </div>
            </div>

            <!-- CONTEXT GRAPH STRUCTURE -->
            <div style="margin-bottom: 16px; padding: 12px; background: rgba(88, 166, 255, 0.1); border-radius: 8px; border: 1px solid rgba(88, 166, 255, 0.2);">
                <div style="font-weight: 600; margin-bottom: 8px; color: #58a6ff;">🔗 Context Graph Structure</div>
                <div style="font-size: 0.8rem; color: #aaa; margin-bottom: 8px;">
                    <strong>Traversal Path:</strong> ${graph_traversal.path || 'AIRPORT ← FLIGHT ← SHIPMENT ← BOOKING → CARRIER'}
                </div>
                <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 8px;">
                    ${Object.entries(edge_types).map(([type, count]) => `
                        <span style="padding: 2px 8px; background: rgba(88, 166, 255, 0.2); border-radius: 4px; font-size: 0.75rem;">
                            ${type}: ${count}
                        </span>
                    `).join('')}
                </div>
                ${carriers.length > 0 ? `
                    <div style="font-size: 0.8rem; color: #aaa;">
                        <strong>Carriers:</strong> ${carriers.map(c => c.iata_code || c.name).join(', ')}
                    </div>
                ` : ''}
            </div>

            <!-- NETWORK POSITION -->
            ${network.connected_airports?.length > 0 ? `
                <div style="margin-bottom: 16px; padding: 12px; background: rgba(23, 162, 184, 0.1); border-radius: 8px; border: 1px solid rgba(23, 162, 184, 0.2);">
                    <div style="font-weight: 600; margin-bottom: 8px; color: #17a2b8;">
                        🌐 Network Position ${network.is_hub ? '<span style="background: #17a2b8; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; margin-left: 8px;">HUB</span>' : ''}
                    </div>
                    <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                        ${network.connected_airports.slice(0, 8).map(a => `
                            <span style="padding: 4px 10px; background: rgba(23, 162, 184, 0.2); border-radius: 4px; font-size: 0.8rem;">
                                ${a.airport} <span style="color: #888;">(${a.flights})</span>
                            </span>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            <!-- CLAIMS (Graph-Derived Assertions) -->
            ${claims.length > 0 ? `
                <div style="margin-bottom: 16px; padding: 12px; background: rgba(255, 193, 7, 0.1); border-radius: 8px; border: 1px solid rgba(255, 193, 7, 0.2);">
                    <div style="font-weight: 600; margin-bottom: 8px; color: #ffc107;">📋 Claims (Graph-Derived)</div>
                    <div style="display: grid; gap: 6px;">
                        ${claims.map(c => `
                            <div style="padding: 8px; background: rgba(0,0,0,0.2); border-radius: 4px; border-left: 3px solid ${c.status === 'FACT' ? '#28a745' : c.status === 'HYPOTHESIS' ? '#ffc107' : '#6c757d'};">
                                <div style="font-size: 0.85rem;">${escapeHtml(c.text)}</div>
                                <div style="display: flex; gap: 12px; margin-top: 4px; font-size: 0.75rem; color: #888;">
                                    <span style="color: ${c.status === 'FACT' ? '#28a745' : '#ffc107'};">${c.status}</span>
                                    <span>${c.confidence}% confidence</span>
                                    <span>Source: ${c.source}</span>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            <!-- EVIDENCE PROVENANCE -->
            ${evidence_sources.length > 0 ? `
                <div style="margin-bottom: 16px; padding: 12px; background: rgba(108, 117, 125, 0.1); border-radius: 8px; border: 1px solid rgba(108, 117, 125, 0.2);">
                    <div style="font-weight: 600; margin-bottom: 8px; color: #6c757d;">🔍 Evidence Provenance</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                        ${evidence_sources.map(s => `
                            <span style="padding: 4px 10px; background: rgba(108, 117, 125, 0.2); border-radius: 4px; font-size: 0.8rem;">
                                ${s.source} <span style="color: #888;">(${s.evidence_count} items)</span>
                            </span>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            <!-- BI-TEMPORAL ASPECTS -->
            ${renderBitemporalSection(impact.bitemporal)}

            ${sla_exposure.length > 0 ? `
                <!-- SLA Exposure (Deadline-based, not "at risk") -->
                <div style="margin-bottom: 16px;">
                    <div style="font-weight: 600; margin-bottom: 4px; color: #dc3545;">SLA Deadline Exposure</div>
                    <div style="font-size: 0.75rem; color: #888; margin-bottom: 8px;">Shipments with SLA deadlines within 24 hours (simulated). OVERDUE = penalty exposure is active. If everything is OVERDUE, click “Refresh Ops Graph”.</div>
                    <div style="display: grid; gap: 6px;">
                        ${sla_exposure.map(s => `
                            <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: rgba(220, 53, 69, 0.1); border-radius: 4px; border-left: 3px solid #dc3545;">
                                <span style="font-family: monospace;">${escapeHtml(s.tracking_number)}</span>
                                <span style="color: #888;">${s.service_level}</span>
                                <span style="color: ${s.hours_remaining < 0 ? '#dc3545' : s.hours_remaining < 12 ? '#ffc107' : '#28a745'}; font-weight: 600;">
                                    ${s.hours_remaining < 0 ? 'OVERDUE' : s.hours_remaining.toFixed(1) + 'h left'}
                                </span>
                                <span style="color: #888; margin-left: 8px;">$${(s.booking_charge || 0).toFixed(0)}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            ${flights.length > 0 ? `
                <!-- Flights -->
                <div style="margin-bottom: 16px;">
                    <div style="font-weight: 600; margin-bottom: 4px; color: #e0e0e0;">Affected Flights</div>
                    <div style="font-size: 0.75rem; color: #888; margin-bottom: 8px;">Flights departing from or arriving at ${escapeHtml(impactAirport)} that carry forwarded cargo</div>
                    <div style="display: grid; gap: 6px;">
                        ${flights.map(f => `
                            <div style="display: flex; justify-content: space-between; padding: 8px 12px; background: rgba(255, 255, 255, 0.05); border-radius: 4px;">
                                <span style="font-family: monospace; font-weight: 600;">${escapeHtml(f.flight_number)}</span>
                                <span>${f.origin} → ${f.destination}</span>
                                <span style="color: ${f.status === 'DELAYED' ? '#ffc107' : f.status === 'CANCELLED' ? '#dc3545' : '#28a745'};">
                                    ${f.status}
                                </span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            ${shipments.length > 0 ? `
                <!-- Shipments -->
                <div>
                    <div style="font-weight: 600; margin-bottom: 4px; color: #e0e0e0;">Affected Shipments</div>
                    <div style="font-size: 0.75rem; color: #888; margin-bottom: 8px;">Shipments booked through this gateway. Booking charge = forwarder revenue (not cargo value). Service level determines SLA penalty exposure.</div>
                    <div style="display: grid; gap: 6px;">
                        ${shipments.map(s => `
                            <div style="padding: 10px 12px; background: rgba(255, 255, 255, 0.05); border-radius: 4px; border-left: 3px solid ${s.service_level === 'EXPRESS' ? '#dc3545' : s.service_level === 'PRIORITY' ? '#ffc107' : '#28a745'};">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <span style="font-family: monospace; font-size: 0.85rem;">${escapeHtml(s.tracking_number)}</span>
                                    <span style="padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; background: ${s.service_level === 'EXPRESS' ? 'rgba(220, 53, 69, 0.2)' : s.service_level === 'PRIORITY' ? 'rgba(255, 193, 7, 0.2)' : 'rgba(40, 167, 69, 0.2)'}; color: ${s.service_level === 'EXPRESS' ? '#dc3545' : s.service_level === 'PRIORITY' ? '#ffc107' : '#28a745'};">
                                        ${s.service_level}
                                    </span>
                                </div>
                                <div style="display: flex; gap: 16px; margin-top: 6px; font-size: 0.8rem; color: #888;">
                                    <span>${escapeHtml(s.commodity)}</span>
                                    <span>${s.weight_kg?.toFixed(0) || 0} kg</span>
                                    <span>Booking: $${(s.booking_charge || 0).toFixed(2)}</span>
                                    <span>Deadline: ${formatDeadline(s.sla_deadline)}</span>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            ${!flights.length && !shipments.length ? `
                <p class="empty-state">No operational data seeded for this airport</p>
            ` : ''}
        </div>
    `;
}

function renderBitemporalSection(bitemporal) {
    if (!bitemporal) return '';

    const temporal_edges = bitemporal.temporal_edges || [];
    const supersession = bitemporal.supersession_chain || [];
    const versions = bitemporal.version_history || [];
    const contradictions = bitemporal.contradictions || [];

    const formatTime = (t) => {
        if (!t || t === 'CURRENT') return t || 'N/A';
        try {
            return new Date(t).toLocaleString();
        } catch { return t; }
    };

    const hasData = temporal_edges.length > 0 || supersession.length > 0 || versions.length > 0 || contradictions.length > 0;
    if (!hasData) return '';

    return `
        <div style="margin-bottom: 16px; padding: 12px; background: rgba(138, 43, 226, 0.1); border-radius: 8px; border: 1px solid rgba(138, 43, 226, 0.2);">
            <div style="font-weight: 600; margin-bottom: 8px; color: #8a2be2;">⏱️ Bi-Temporal Graph</div>
            <div style="font-size: 0.75rem; color: #888; margin-bottom: 12px;">
                Event Time (when it was/will be true) vs Ingest Time (when we learned it)
            </div>

            ${temporal_edges.length > 0 ? `
                <div style="margin-bottom: 12px;">
                    <div style="font-size: 0.8rem; font-weight: 600; margin-bottom: 6px; color: #aaa;">Temporal Edges (validity windows)</div>
                    <div style="display: grid; gap: 4px; font-size: 0.75rem;">
                        ${temporal_edges.slice(0, 5).map(e => `
                            <div style="display: flex; gap: 8px; padding: 6px 8px; background: rgba(0,0,0,0.2); border-radius: 4px;">
                                <span style="color: #8a2be2; min-width: 180px;">${e.edge_type}</span>
                                <span style="color: #888;">Event: ${e.event_time_start ? formatTime(e.event_time_start).split(',')[0] : 'NULL'}</span>
                                <span style="color: #888;">Ingested: ${formatTime(e.ingested_at).split(',')[0]}</span>
                                <span style="color: ${e.status === 'FACT' ? '#28a745' : '#ffc107'};">${e.status}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            ${supersession.length > 0 ? `
                <div style="margin-bottom: 12px;">
                    <div style="font-size: 0.8rem; font-weight: 600; margin-bottom: 6px; color: #aaa;">Claim Supersession Chain (audit trail)</div>
                    <div style="display: grid; gap: 4px; font-size: 0.75rem;">
                        ${supersession.map(s => `
                            <div style="padding: 6px 8px; background: rgba(0,0,0,0.2); border-radius: 4px; border-left: 3px solid #8a2be2;">
                                <div style="color: #e0e0e0;">${escapeHtml(s.current_claim)}</div>
                                <div style="color: #888; margin-top: 4px;">
                                    ↑ Supersedes: "${escapeHtml(s.supersedes_claim)}"
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            ${versions.length > 0 ? `
                <div style="margin-bottom: 12px;">
                    <div style="font-size: 0.8rem; font-weight: 600; margin-bottom: 6px; color: #aaa;">Node Version History</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 6px; font-size: 0.75rem;">
                        ${versions.map(v => `
                            <span style="padding: 4px 8px; background: rgba(138, 43, 226, 0.2); border-radius: 4px;">
                                ${v.valid_to === 'CURRENT' ? '●' : '○'} ${formatTime(v.valid_from).split(',')[0]}
                                ${v.valid_to !== 'CURRENT' ? ` → ${formatTime(v.valid_to).split(',')[0]}` : ' (current)'}
                            </span>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            ${contradictions.length > 0 ? `
                <div>
                    <div style="font-size: 0.8rem; font-weight: 600; margin-bottom: 6px; color: #dc3545;">⚠️ Temporal Contradictions</div>
                    <div style="display: grid; gap: 4px; font-size: 0.75rem;">
                        ${contradictions.map(c => `
                            <div style="padding: 6px 8px; background: rgba(220, 53, 69, 0.1); border-radius: 4px; border-left: 3px solid #dc3545;">
                                <div style="display: flex; justify-content: space-between;">
                                    <span style="color: ${c.status === 'OPEN' ? '#dc3545' : '#28a745'};">${c.status}</span>
                                    <span style="color: #888;">${formatTime(c.detected_at).split(',')[0]}</span>
                                </div>
                                <div style="color: #e0e0e0; margin-top: 4px;">${escapeHtml(c.claim_a)}</div>
                                <div style="color: #ffc107;">vs ${escapeHtml(c.claim_b)}</div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}
        </div>
    `;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Toggle collapsible sections
function toggleSection(sectionId) {
    const content = document.getElementById(`packet-${sectionId}`);
    const toggle = document.getElementById(`${sectionId}-toggle`);

    if (!content || !toggle) return;

    const isCollapsed = content.classList.contains('collapsed');

    if (isCollapsed) {
        content.classList.remove('collapsed');
        content.classList.add('expanded');
        toggle.classList.remove('rotated');
    } else {
        content.classList.add('collapsed');
        content.classList.remove('expanded');
        toggle.classList.add('rotated');
    }
}

// Make selectCase and toggleSection available globally for onclick
window.selectCase = selectCase;
window.toggleSection = toggleSection;

// ============================================================
// SIMULATION FUNCTIONALITY
// ============================================================

const scenarioSelect = document.getElementById('scenario-select');
const btnRunSimulation = document.getElementById('btn-run-simulation');
const btnRunAllSimulations = document.getElementById('btn-run-all-simulations');
const scenarioDetails = document.getElementById('scenario-details');
const simulationResults = document.getElementById('simulation-results');

// Scenario data cache
let scenariosData = null;
let currentScenario = null;

// Initialize simulation on page load
if (scenarioSelect) {
    loadScenarios();
    setupSimulationListeners();
}

function setupSimulationListeners() {
    if (scenarioSelect) {
        scenarioSelect.addEventListener('change', handleScenarioChange);
    }
    if (btnRunSimulation) {
        btnRunSimulation.addEventListener('click', handleRunSimulation);
    }
    if (btnRunAllSimulations) {
        btnRunAllSimulations.addEventListener('click', handleRunAllSimulations);
    }
}

async function loadScenarios() {
    try {
        const result = await apiCall('/simulation/scenarios');
        scenariosData = result;

        scenarioSelect.innerHTML = '<option value="">-- Select a scenario --</option>';

        result.scenarios.forEach(s => {
            const option = document.createElement('option');
            option.value = s.id;
            option.textContent = `${s.airport_icao} - ${s.name}`;
            if (s.has_contradiction) option.textContent += ' ⚠️';
            if (s.has_missing_source) option.textContent += ' ⏳';
            scenarioSelect.appendChild(option);
        });

        btnRunAllSimulations.disabled = false;
    } catch (error) {
        console.error('Failed to load scenarios:', error);
        scenarioSelect.innerHTML = '<option value="">-- Failed to load scenarios --</option>';
    }
}

async function handleScenarioChange() {
    const scenarioId = scenarioSelect.value;

    if (!scenarioId) {
        scenarioDetails.classList.add('hidden');
        btnRunSimulation.disabled = true;
        currentScenario = null;
        return;
    }

    try {
        const scenario = await apiCall(`/simulation/scenarios/${scenarioId}`);
        currentScenario = scenario;
        displayScenarioDetails(scenario);
        btnRunSimulation.disabled = false;
    } catch (error) {
        console.error('Failed to load scenario details:', error);
        showStatus(`Failed to load scenario: ${error.message}`, 'error');
    }
}

function displayScenarioDetails(scenario) {
    document.getElementById('scenario-airport').textContent = scenario.airport_icao;

    const postureEl = document.getElementById('scenario-expected-posture');
    postureEl.textContent = scenario.expected_posture;
    postureEl.className = `posture-badge small ${scenario.expected_posture}`;

    document.getElementById('scenario-risk-level').textContent = scenario.expected_risk_level;
    document.getElementById('scenario-description').textContent = scenario.description;

    const flagContradiction = document.getElementById('flag-contradiction');
    const flagMissing = document.getElementById('flag-missing');

    if (scenario.has_contradiction) {
        flagContradiction.classList.remove('hidden');
    } else {
        flagContradiction.classList.add('hidden');
    }

    if (scenario.has_missing_source) {
        flagMissing.classList.remove('hidden');
        flagMissing.textContent = `⏳ Missing: ${scenario.missing_source}`;
    } else {
        flagMissing.classList.add('hidden');
    }

    scenarioDetails.classList.remove('hidden');
}

async function handleRunSimulation() {
    if (!currentScenario) return;

    btnRunSimulation.disabled = true;
    btnRunSimulation.textContent = 'Running...';
    showStatus(`Running simulation: ${currentScenario.name}...`, 'info');

    try {
        const result = await apiCall(`/simulation/run/${currentScenario.id}`, 'POST');
        displaySingleResult(result);

        if (result.passed) {
            showStatus(`Simulation PASSED: ${currentScenario.name}`, 'success');
        } else {
            showStatus(
                `Simulation FAILED: Expected ${result.expected_posture}, got ${result.actual_posture}`,
                'error'
            );
        }
    } catch (error) {
        console.error('Simulation failed:', error);
        showStatus(`Simulation error: ${error.message}`, 'error');
    } finally {
        btnRunSimulation.disabled = false;
        btnRunSimulation.textContent = 'Run Scenario';
    }
}

async function handleRunAllSimulations() {
    btnRunAllSimulations.disabled = true;
    btnRunAllSimulations.textContent = 'Running... (~2 min)';
    btnRunSimulation.disabled = true;
    showStatus('Running all 10 scenarios sequentially with REAL LLM calls... (~2-3 minutes total)', 'info');

    // Show a progress indicator
    const startTime = Date.now();
    const progressInterval = setInterval(() => {
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        const mins = Math.floor(elapsed / 60);
        const secs = elapsed % 60;
        btnRunAllSimulations.textContent = `Running... ${mins}:${secs.toString().padStart(2, '0')}`;
    }, 1000);

    try {
        const result = await apiCall('/simulation/run-all');
        clearInterval(progressInterval);
        displayBatchResults(result);

        const passRate = Math.round((result.passed / result.total) * 100);
        showStatus(
            `All simulations complete: ${result.passed}/${result.total} passed (${passRate}%)`,
            result.failed === 0 ? 'success' : 'warning'
        );
    } catch (error) {
        clearInterval(progressInterval);
        console.error('Batch simulation failed:', error);
        showStatus(`Batch simulation error: ${error.message}`, 'error');
    } finally {
        btnRunAllSimulations.disabled = false;
        btnRunAllSimulations.textContent = 'Run All Scenarios';
        btnRunSimulation.disabled = !currentScenario;
    }
}

function displaySingleResult(result) {
    simulationResults.classList.remove('hidden');

    document.getElementById('sim-total').textContent = '1';
    document.getElementById('sim-passed').textContent = result.passed ? '1' : '0';
    document.getElementById('sim-failed').textContent = result.passed ? '0' : '1';
    document.getElementById('sim-pass-rate').textContent = result.passed ? '100%' : '0%';

    const resultsList = document.getElementById('results-list');
    resultsList.innerHTML = renderResultItem(result);
}

function displayBatchResults(batch) {
    simulationResults.classList.remove('hidden');

    document.getElementById('sim-total').textContent = batch.total;
    document.getElementById('sim-passed').textContent = batch.passed;
    document.getElementById('sim-failed').textContent = batch.failed;
    document.getElementById('sim-pass-rate').textContent = batch.pass_rate;

    const resultsList = document.getElementById('results-list');
    resultsList.innerHTML = batch.results.map(renderResultItem).join('');
}

function renderResultItem(result) {
    const passedClass = result.passed ? 'passed' : 'failed';
    const icon = result.passed ? '✓' : '✗';

    return `
        <div class="result-item ${passedClass}">
            <div class="result-header">
                <span class="result-icon">${icon}</span>
                <span class="result-airport">${result.airport_icao}</span>
                <span class="result-name">${result.scenario_name}</span>
            </div>
            <div class="result-postures">
                <span class="posture-label">Expected:</span>
                <span class="posture-badge small ${result.expected_posture}">${result.expected_posture}</span>
                <span class="posture-label">Actual:</span>
                <span class="posture-badge small ${result.actual_posture || 'UNKNOWN'}">${result.actual_posture || '--'}</span>
            </div>
            <div class="result-metrics">
                <span>PDL: ${result.pdl_seconds?.toFixed(1) || '--'}s</span>
                <span>Evidence: ${result.evidence_count}</span>
                <span>Claims: ${result.claim_count}</span>
                ${result.contradiction_count > 0 ? `<span class="warning">Contradictions: ${result.contradiction_count}</span>` : ''}
            </div>
            ${result.error ? `<div class="result-error">${escapeHtml(result.error)}</div>` : ''}
        </div>
    `;
}
