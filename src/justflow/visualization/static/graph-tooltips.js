/*
 * Shared Justflow graph tooltip engine. This single file is inlined by the
 * CLI visualization export (justflow graph) and served verbatim to the
 * administration panel's graph frame, so the two cannot drift.
 */
(function () {
    'use strict';

    function appendTextElement(parent, tagName, className, value) {
        const element = document.createElement(tagName);
        if (className) element.className = className;
        element.textContent = String(value);
        parent.appendChild(element);
        return element;
        }

    function renderEmptyState(container, message) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = message;
        container.replaceChildren(empty);
        }

    function attach(config) {
        const tooltip = config.tooltipElement;
        const nodeMetadata = config.nodeMetadata || {};
        const edgeMetadata = config.edgeMetadata || [];
        const knownNodeIds = Object.keys(nodeMetadata);


        function resetTooltip(title, badge) {
            tooltip.replaceChildren();
            const header = document.createElement('div');
            header.className = 'tt-header';
            header.appendChild(document.createTextNode(title + ' '));
            appendTextElement(header, 'span', 'tt-badge decision', badge);
            tooltip.appendChild(header);
        }

        function appendTooltipRow(label, value) {
            const row = document.createElement('div');
            row.className = 'tt-row';
            appendTextElement(row, 'span', 'tt-label', label);
            appendTextElement(row, 'span', 'tt-value', value);
            tooltip.appendChild(row);
        }

        function extractNodeId(element) {
            // Strategy 1: data-id attribute (Mermaid v11.x)
            if (element.dataset && element.dataset.id) return element.dataset.id;

            // Strategy 2: parse from element ID (format: "flowchart-<name>-<counter>")
            const id = element.id || '';
            if (id) {
                // Try matching against known node IDs (handles names with hyphens)
                for (const knownId of knownNodeIds) {
                    if (id.includes(knownId)) return knownId;
                }
                // Fallback regex: strip prefix and trailing number
                const match = id.match(/^(?:flowchart-)?(.+?)(?:-\\d+)?$/);
                if (match) return match[1];
            }

            // Strategy 3: match by label text content
            const labelEl = element.querySelector('.nodeLabel') || element.querySelector('text') || element.querySelector('span');
            if (labelEl) {
                const text = labelEl.textContent.trim();
                for (const knownId of knownNodeIds) {
                    const meta = nodeMetadata[knownId];
                    if (text === knownId || text === (meta && meta.reason ? knownId + ': ' + meta.reason : knownId)) {
                        return knownId;
                    }
                }
            }
            return null;
        }

        // Click a node to pin its tooltip open; click it again (or anywhere
        // else) to unpin.
        let pinnedNodeId = null;

        function unpin() {
            pinnedNodeId = null;
            tooltip.classList.remove('pinned');
            tooltip.classList.remove('visible');
        }

        function attachTooltips() {
            const svg = config.svg;
            if (!svg) return;

            const nodes = svg.querySelectorAll('.node');
            nodes.forEach(node => {
                const nodeId = extractNodeId(node);
                if (!nodeId || !nodeMetadata[nodeId]) return;

                node.style.cursor = 'pointer';
                node.addEventListener('mouseenter', (e) => showTooltip(nodeId, e));
                node.addEventListener('mousemove', (e) => positionTooltip(e));
                node.addEventListener('mouseleave', hideTooltip);
                node.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (pinnedNodeId === nodeId) {
                        unpin();
                        return;
                    }
                    pinnedNodeId = null;
                    showTooltip(nodeId, e);
                    pinnedNodeId = nodeId;
                    tooltip.classList.add('pinned');
                });
            });

            document.addEventListener('click', () => {
                if (pinnedNodeId) unpin();
            });

            attachDataPlaneToggle(svg);

            // Edge label hover
            const edgeLabels = svg.querySelectorAll('.edgeLabel');
            edgeLabels.forEach(label => {
                const textEl = label.querySelector('span') || label.querySelector('text');
                if (!textEl) return;
                const text = textEl.textContent.trim();
                if (!text) return;

                const meta = edgeMetadata.find(e => e.short_label === text);
                if (!meta) return;

                label.style.cursor = 'pointer';
                const rect = label.querySelector('rect');
                if (rect) {
                    rect.setAttribute('fill', '#f5f3ff');
                    rect.setAttribute('stroke', '#c4b5fd');
                    rect.setAttribute('stroke-width', '1');
                }

                label.addEventListener('mouseenter', (e) => showEdgeTooltip(meta, e));
                label.addEventListener('mousemove', (e) => positionTooltip(e));
                label.addEventListener('mouseleave', hideTooltip);
            });

            // Edge path hover — wider invisible hit area + highlight on hover
            const edgePaths = svg.querySelectorAll('.edgePath');
            edgePaths.forEach((edgePath, idx) => {
                if (idx >= edgeMetadata.length) return;
                const meta = edgeMetadata[idx];
                const path = edgePath.querySelector('path');
                if (!path) return;

                const d = path.getAttribute('d');
                if (!d) return;

                // Create a wider invisible hit-area path via SVG namespace
                const ns = 'http://www.w3.org/2000/svg';
                const hitArea = document.createElementNS(ns, 'path');
                hitArea.setAttribute('d', d);
                hitArea.setAttribute('stroke', 'transparent');
                hitArea.setAttribute('stroke-width', '20');
                hitArea.setAttribute('fill', 'none');
                hitArea.classList.add('edge-hitarea');
                edgePath.appendChild(hitArea);

                hitArea.addEventListener('mouseenter', (e) => {
                    path.style.stroke = '#6366f1';
                    path.style.strokeWidth = '3px';
                    path.style.transition = 'stroke 0.15s, stroke-width 0.15s';
                    showEdgeTooltip(meta, e);
                });
                hitArea.addEventListener('mousemove', (e) => positionTooltip(e));
                hitArea.addEventListener('mouseleave', () => {
                    path.style.stroke = '';
                    path.style.strokeWidth = '';
                    hideTooltip();
                });
            });
        }

        function showEdgeTooltip(meta, event) {
            if (pinnedNodeId) return;
            resetTooltip('Edge', 'edge');
            appendTooltipRow('From', meta.source);
            appendTooltipRow('To', meta.target);
            if (meta.full_label) appendTooltipRow('Condition', meta.full_label);
            tooltip.classList.add('visible');
            positionTooltip(event);
        }

        function attachDataPlaneToggle(svg) {
            const toggle = config.dataPlaneToggle || null;
            if (!toggle) return;

            const resourceNodes = [...svg.querySelectorAll('.node')].filter(n => {
                const nodeId = extractNodeId(n);
                return nodeId && nodeMetadata[nodeId]?.type === 'resource';
            });
            const resourceEdgeIdx = new Set(
                edgeMetadata.map((e, i) => e.kind === 'resource' ? i : -1).filter(i => i >= 0));
            const resourceEdges = [...svg.querySelectorAll('.edgePath')].filter(
                (_, i) => resourceEdgeIdx.has(i));
            const resourceLabels = [...svg.querySelectorAll('.edgeLabel')].filter(l => {
                const t = (l.textContent || '').trim();
                return t === 'uses' || t === 'cache' || t === 'audit';
            });
            if (resourceNodes.length === 0) {
                toggle.parentElement.parentElement.style.display = 'none';
                return;
            }

            toggle.addEventListener('change', () => {
                const display = toggle.checked ? '' : 'none';
                [...resourceNodes, ...resourceEdges, ...resourceLabels].forEach(
                    el => el.style.display = display);
            });
        }

        function showTooltip(nodeId, event) {
            if (pinnedNodeId && pinnedNodeId !== nodeId) return;
            const meta = nodeMetadata[nodeId];
            if (!meta) return;

            resetTooltip(meta.id, meta.type);

            const fields = [
                ['workflow', 'Sub-workflow'],
                ['service', 'Service'],
                ['action', 'Action'],
                ['transport', 'Transport'],
                ['class', 'Class'],
                ['input', 'Input'],
                ['output', 'Output'],
                ['condition', 'Condition'],
                ['for_each', 'For each'],
                ['as', 'As var'],
                ['parallel', 'Parallel'],
                ['max_concurrency', 'Max conc.'],
                ['on_iteration_fail', 'On fail'],
                ['reason', 'Reason'],
            ];

            for (const [key, label] of fields) {
                if (meta[key] !== undefined && meta[key] !== null && meta[key] !== false) {
                    let val = meta[key];
                    if (typeof val === 'object') val = JSON.stringify(val);
                    appendTooltipRow(label, val);
                }
            }

            if (meta.params && Object.keys(meta.params).length > 0) {
                appendTooltipRow('Params', JSON.stringify(meta.params, null, 2));
            }

            tooltip.classList.add('visible');
            positionTooltip(event);
        }

        function positionTooltip(event) {
            if (pinnedNodeId) return;
            tooltip.style.left = '-9999px';
            tooltip.style.top = '-9999px';
            tooltip.style.display = 'block';
            const rect = tooltip.getBoundingClientRect();
            let x = event.clientX + 14;
            let y = event.clientY + 14;
            if (x + rect.width > window.innerWidth - 20) x = event.clientX - rect.width - 14;
            if (y + rect.height > window.innerHeight - 20) y = event.clientY - rect.height - 14;
            tooltip.style.left = x + 'px';
            tooltip.style.top = y + 'px';
        }

        function hideTooltip() {
            if (pinnedNodeId) return;
            tooltip.classList.remove('visible');
        }

        attachTooltips();
    }

    window.JustflowGraphTooltips = {
        attach: attach,
        appendTextElement: appendTextElement,
        renderEmptyState: renderEmptyState,
    };
})();
