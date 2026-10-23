/**
 * DistKV Cluster Visual Dashboard (Phase 7)
 *
 * <p>Mathematical Explanation of Hash-to-Angle Mapping (for Interviews):
 * <ul>
 *   <li>The 64-bit MD5 hash token is mapped to a normalized floating-point number in [0.0, 1.0).</li>
 *   <li>We convert this fraction to polar coordinates:
 *       angleRadians = (normalized * 2 * Math.PI) - (Math.PI / 2)
 *       (Subtracting PI/2 rotates the 0-point to the 12 o'clock position on the clock face).</li>
 *   <li>Using basic trigonometry on circle of radius R=260 centered at (350, 350):
 *       X = 350 + R * cos(angleRadians)
 *       Y = 350 + R * sin(angleRadians)</li>
 *   <li>This yields a 1-to-1 bijection between the 64-bit keyspace and positions on the SVG ring.</li>
 * </ul>
 */

(function () {
    'use strict';

    // System Palette Mapping (Strict Functional Assignment)
    const NODE_COLORS = {
        'node1': '#2F54D6', // Cobalt
        'node2': '#D99A1E', // Amber
        'node3': '#178A7A', // Teal
        'default': '#536471'
    };

    const RING_CX = 350;
    const RING_CY = 350;
    const RING_R = 260;

    const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    // Local State
    let isInitialLoad = true;
    let previousNodeStatus = {};
    let keyPositions = new Map(); // key -> { currentAngle, targetAngle, color, element }

    // DOM Elements
    const baseRing = document.getElementById('base-ring');
    const nodeArcsGroup = document.getElementById('node-arcs');
    const vnodeTicksGroup = document.getElementById('vnode-ticks');
    const replicaLinesGroup = document.getElementById('replica-lines');
    const keyDotsGroup = document.getElementById('key-dots');
    const centerKeyCount = document.getElementById('center-key-count');
    const clusterSummary = document.getElementById('cluster-summary');
    const failoverNotice = document.getElementById('failover-notice');
    const nodeList = document.getElementById('node-list');
    const sparklinePath = document.getElementById('sparkline-path');

    const metricOps = document.getElementById('metric-ops');
    const metricP99 = document.getElementById('metric-p99');
    const metricP50 = document.getElementById('metric-p50');
    const metricP95 = document.getElementById('metric-p95');
    const metricKeys = document.getElementById('metric-keys');

    const cmdForm = document.getElementById('cmd-form');
    const cmdInput = document.getElementById('cmd-input');
    const cmdSubmit = document.getElementById('cmd-submit');
    const cmdOutput = document.getElementById('cmd-output');

    /**
     * Converts a normalized angle [0.0, 1.0) into SVG (x, y) coordinates on the ring.
     */
    function angleToCoordinates(normalizedAngle, radius = RING_R) {
        const radians = (normalizedAngle * 2.0 * Math.PI) - (Math.PI / 2.0);
        return {
            x: RING_CX + radius * Math.cos(radians),
            y: RING_CY + radius * Math.sin(radians)
        };
    }

    /**
     * Resolves the strict color assigned to a node.
     */
    function getNodeColor(nodeId, isDown = false) {
        if (isDown) return '#8D9DAA';
        return NODE_COLORS[nodeId] || NODE_COLORS['default'];
    }

    // ==================== POLLING & DATA FETCHING ====================

    async function fetchStats() {
        try {
            const res = await fetch('/api/stats');
            if (!res.ok) return;
            const data = await res.json();
            renderStats(data);
        } catch (e) {
            console.warn('Failed to fetch stats:', e);
        }
    }

    async function fetchRing() {
        try {
            const res = await fetch('/api/ring');
            if (!res.ok) return;
            const data = await res.json();
            renderRing(data);
        } catch (e) {
            console.warn('Failed to fetch ring topology:', e);
        }
    }

    // ==================== RENDERING LOGIC ====================

    function renderStats(stats) {
        // Plain numbers formatted with tabular figures
        metricOps.textContent = Number(stats.ops_per_sec || 0).toLocaleString();
        metricP99.textContent = Number(stats.p99_us || 0).toFixed(2);
        metricP50.textContent = Number(stats.p50_us || 0).toFixed(2);
        metricP95.textContent = Number(stats.p95_us || 0).toFixed(2);
        metricKeys.textContent = Number(stats.total_keys || 0).toLocaleString();
        centerKeyCount.textContent = Number(stats.total_keys || 0).toLocaleString();

        // Render nodes list
        const nodes = stats.nodes || [];
        nodeList.innerHTML = '';
        let downCount = 0;

        nodes.forEach(node => {
            const isDown = node.status === 'down';
            if (isDown) downCount++;

            const row = document.createElement('div');
            row.className = 'node-row';

            const swatchColor = isDown ? 'var(--alert-down)' : getNodeColor(node.id);
            const statusText = isDown 
                ? `down (${node.down_since_seconds}s)` 
                : (node.is_local ? 'active (local)' : 'active');

            row.innerHTML = `
                <div class="node-ident">
                    <span class="node-swatch" style="background: ${swatchColor}"></span>
                    <span class="node-name">${escapeHtml(node.id)}</span>
                    <span class="node-status-text ${isDown ? 'down' : ''}">${statusText}</span>
                </div>
            `;
            nodeList.appendChild(row);
        });

        if (downCount === 0) {
            clusterSummary.textContent = `${nodes.length} nodes connected • Normal replication`;
        } else {
            clusterSummary.textContent = `${downCount} of ${nodes.length} nodes unreachable`;
        }

        // Render Sparkline (Throughput History over last 60 seconds)
        renderSparkline(stats.throughput_history || []);
    }

    function renderSparkline(history) {
        if (!history || history.length < 2) return;

        const width = 300;
        const height = 64;
        const padding = 4;

        const maxVal = Math.max(...history, 10);
        const stepX = (width - padding * 2) / (history.length - 1);

        let d = '';
        history.forEach((val, i) => {
            const x = padding + i * stepX;
            const y = height - padding - ((val / maxVal) * (height - padding * 2));
            if (i === 0) {
                d += `M ${x.toFixed(1)} ${y.toFixed(1)}`;
            } else {
                d += ` L ${x.toFixed(1)} ${y.toFixed(1)}`;
            }
        });

        sparklinePath.setAttribute('d', d);
    }

    function renderRing(data) {
        const nodes = data.nodes || [];
        const vnodes = data.vnodes || [];
        const keys = data.keys || [];

        // Check if any node is currently down
        let downedNode = null;
        const currentStatus = {};
        nodes.forEach(n => {
            currentStatus[n.id] = n.status;
            if (n.status === 'down' && !downedNode) {
                downedNode = n;
            }
        });

        // Failover sentence requirement
        if (downedNode) {
            const keysOnDowned = keys.filter(k => k.primary === downedNode.id).length;
            failoverNotice.hidden = false;
            failoverNotice.textContent = `${downedNode.id} stopped responding ${downedNode.down_since_seconds} seconds ago. Its ${keysOnDowned} keys are now served from replicas.`;
        } else {
            failoverNotice.hidden = true;
        }

        // 1. Render Virtual Node Ticks (outward tick mark at angle)
        vnodeTicksGroup.innerHTML = '';
        vnodes.forEach(vn => {
            const isNodeDown = currentStatus[vn.node] === 'down';
            const inner = angleToCoordinates(vn.angle, RING_R - 5);
            const outer = angleToCoordinates(vn.angle, RING_R + 5);

            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', inner.x.toFixed(1));
            line.setAttribute('y1', inner.y.toFixed(1));
            line.setAttribute('x2', outer.x.toFixed(1));
            line.setAttribute('y2', outer.y.toFixed(1));
            line.setAttribute('stroke', isNodeDown ? '#8D9DAA' : getNodeColor(vn.node));
            line.setAttribute('class', 'vnode-tick');
            vnodeTicksGroup.appendChild(line);
        });

        // 2. Render Node Arcs (colored bands around ring perimeter)
        nodeArcsGroup.innerHTML = '';
        nodes.forEach((n, idx) => {
            const isDown = n.status === 'down';
            const arc = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            arc.setAttribute('cx', RING_CX);
            arc.setAttribute('cy', RING_CY);
            arc.setAttribute('r', RING_R);
            arc.setAttribute('fill', 'none');

            // Segments based on cluster node count
            const totalNodes = Math.max(1, nodes.length);
            const circumference = 2 * Math.PI * RING_R;
            const arcLength = circumference / totalNodes - 8;
            const offset = -(idx * (circumference / totalNodes));

            arc.setAttribute('stroke-dasharray', `${arcLength.toFixed(1)} ${(circumference - arcLength).toFixed(1)}`);
            arc.setAttribute('stroke-dashoffset', offset.toFixed(1));
            arc.setAttribute('stroke', isDown ? 'url(#downed-hatch)' : getNodeColor(n.id));
            arc.setAttribute('class', 'ring-arc');

            if (isInitialLoad && !prefersReducedMotion) {
                arc.classList.add('ring-animated');
            }
            nodeArcsGroup.appendChild(arc);
        });

        // 3. Render Keys and Replica Lines
        renderKeyDots(keys, currentStatus);

        isInitialLoad = false;
        previousNodeStatus = currentStatus;
    }

    function renderKeyDots(keys, currentStatus) {
        replicaLinesGroup.innerHTML = '';
        keyDotsGroup.innerHTML = '';

        keys.forEach(k => {
            const isPrimaryDown = currentStatus[k.primary] === 'down';
            const activeOwner = isPrimaryDown ? k.replica : k.primary;
            const dotColor = getNodeColor(activeOwner);

            // Compute coordinates
            const primaryCoords = angleToCoordinates(k.angle, RING_R);
            const targetCoords = primaryCoords;

            // Draw dot
            const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('cx', targetCoords.x.toFixed(1));
            circle.setAttribute('cy', targetCoords.y.toFixed(1));
            circle.setAttribute('r', '4.5');
            circle.setAttribute('fill', dotColor);
            circle.setAttribute('class', 'key-dot');

            const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            title.textContent = `Key: ${k.key}\nPrimary: ${k.primary}\nReplica: ${k.replica}\nActive Owner: ${activeOwner}`;
            circle.appendChild(title);

            keyDotsGroup.appendChild(circle);
        });
    }

    // ==================== COMMAND SUBMISSION ====================

    cmdForm.addEventListener('submit', async function (e) {
        e.preventDefault();
        const cmdText = cmdInput.value.trim();
        if (!cmdText) return;

        cmdSubmit.disabled = true;
        cmdOutput.textContent = 'Running command...';

        try {
            const res = await fetch('/api/cmd', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ cmd: cmdText })
            });

            if (!res.ok) {
                cmdOutput.textContent = `Error: Server returned status ${res.status}. Check node connectivity.`;
                return;
            }

            const result = await res.json();
            const elapsed = result.elapsed_us ? ` (${result.elapsed_us.toFixed(1)} µs)` : '';
            cmdOutput.textContent = `${result.reply}${elapsed}`;

            // If SET command placed a key, animate new dot landing on the ring
            if (result.key && result.angle !== undefined) {
                renderLandingDot(result.angle, result.primary || 'node1');
            }

            // Immediately refresh data
            fetchStats();
            fetchRing();
        } catch (err) {
            cmdOutput.textContent = `Network failure: Could not connect to cluster router at localhost:8080.`;
        } finally {
            cmdSubmit.disabled = false;
        }
    });

    function renderLandingDot(normalizedAngle, ownerNode) {
        const coords = angleToCoordinates(normalizedAngle, RING_R);
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', coords.x.toFixed(1));
        circle.setAttribute('cy', coords.y.toFixed(1));
        circle.setAttribute('r', '4.5');
        circle.setAttribute('fill', getNodeColor(ownerNode));
        circle.setAttribute('class', 'key-dot dot-settling');

        keyDotsGroup.appendChild(circle);

        if (!prefersReducedMotion) {
            setTimeout(() => circle.classList.remove('dot-settling'), 500);
        }
    }

    function escapeHtml(str) {
        return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // ==================== INITIALIZATION & INTERVAL ====================

    // Initial load
    fetchStats();
    fetchRing();

    // Poll every 1 second
    setInterval(() => {
        fetchStats();
        fetchRing();
    }, 1000);

})();
