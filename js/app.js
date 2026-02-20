// ============================================
// CONFIGURATION & CONSTANTS
// ============================================
const CONFIG = {
  API_BASE_URL: "https://shacl-api-docker.onrender.com/api",
  TEMP_NS: "https://falcontologist.github.io/shacl-demo/temp/",
  COLORS: {
    class: "#10b981",
    instance: "#f59e0b",
    literal: "#3b82f6"
  },
  GRAPH: {
    LINK_DISTANCE: 220,
    CHARGE_STRENGTH: -1500,
    COLLIDE_RADIUS: 80,
    NODE_SIZES: {
      class: 14,
      instance: 22,
      literal: 10
    }
  }
};

const PREFIXES = `@prefix :    <https://falcontologist.github.io/shacl-demo/ontology/> .
@prefix temp: <${CONFIG.TEMP_NS}> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

`;

// ============================================
// STATE MANAGEMENT
// ============================================
const State = {
  situationMap: new Map(),
  currentSenses: [],
  globalCount: 0,
  selectedSituation: null,
  currentVerb: "",
  
  // Graph state
  simulation: null,
  svg: null,
  g: null,
  zoom: null,
  
  reset() {
    this.currentSenses = [];
    this.selectedSituation = null;
    this.currentVerb = "";
  }
};

// ============================================
// INITIALIZATION
// ============================================
function checkDependencies() {
  if (typeof d3 === 'undefined') {
    console.log("D3 not found. Injecting script...");
    const script = document.createElement('script');
    script.src = "https://d3js.org/d3.v7.min.js";
    script.onload = () => {
      console.log("✓ D3 loaded successfully");
      init();
    };
    script.onerror = () => {
      console.error("✗ Failed to load D3");
      showError("Failed to load D3.js. Visualization will not work.");
      init(); 
    };
    document.head.appendChild(script);
  } else {
    init();
  }
}

function init() {
  console.log("Initializing application...");
  initGraph();
  setupEventListeners();
  setupTTLArea();
  updateGraph();
  fetchStats();
  window.addEventListener('resize', debounce(handleResize, 250));
  console.log("✓ Application initialized");
}

// ============================================
// EVENT LISTENERS
// ============================================
function setupEventListeners() {
  // Verb lookup
  const lookupBtn = getElement('lookupBtn');
  const verbInput = getElement('verbInput');
  if (lookupBtn) lookupBtn.addEventListener('click', handleLookup);
  if (verbInput) {
    verbInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') handleLookup();
    });
  }

  // Sense and situation selection
  const senseSelect = getElement('senseSelect');
  const sitSelect = getElement('situationSelect');
  if (senseSelect) senseSelect.addEventListener('change', handleSenseSelect);
  if (sitSelect) sitSelect.addEventListener('change', handleSituationSelect);

  // Actions
  const addBtn = getElement('addEntryBtn');
  const inferBtn = getElement('inferBtn');
  const validateBtn = getElement('validateBtn');
  if (addBtn) addBtn.addEventListener('click', addEntry);
  if (inferBtn) inferBtn.addEventListener('click', runInference);
  if (validateBtn) validateBtn.addEventListener('click', validateGraph);

  // Utilities
  const templateBtn = getElement('templateBtn');
  const cancelBtn = getElement('cancelBtn');
  const downloadBtn = getElement('downloadBtn');
  const newGraphBtn = getElement('newGraphBtn');
  
  if (templateBtn) templateBtn.addEventListener('click', handleDownloadTemplate);
  if (cancelBtn) cancelBtn.addEventListener('click', resetUI);
  if (downloadBtn) downloadBtn.addEventListener('click', downloadTTL);
  if (newGraphBtn) newGraphBtn.addEventListener('click', handleNewGraph);
  const saveBtn = getElement('saveBtn');
  if (saveBtn) saveBtn.addEventListener('click', saveToGraph);
}

function setupTTLArea() {
  const ttlInput = getElement('ttlInput');
  if (ttlInput) {
    ttlInput.value = PREFIXES;
    ttlInput.addEventListener('input', debounce(updateGraph, 300));
  }
}

// ============================================
// API FUNCTIONS
// ============================================
async function fetchStats() {
  const apiText = getElement('statusAPI');
  const apiDot = getElement('statusDot');
  
  updateStatus(apiText, "Connecting...", "#94a3b8");

  try {
    // Fetch stats
    const statsResp = await fetch(`${CONFIG.API_BASE_URL}/stats`);
    if (!statsResp.ok) throw new Error(`Stats endpoint returned ${statsResp.status}`);
    
    const stats = await statsResp.json();
    
    // Update UI
    updateStatus(apiText, "Online", "#10b981");
    if (apiDot) apiDot.classList.add('online');
    
    ['Shapes', 'Roles', 'Rules', 'Lemmas', 'Senses'].forEach(key => {
      const el = getElement(`count${key}`);
      if (el) el.textContent = stats[key.toLowerCase()] || 0;
    });

    // Fetch form definitions
    const formResp = await fetch(`${CONFIG.API_BASE_URL}/forms`);
    if (formResp.ok) {
      const formData = await formResp.json();
      Object.entries(formData.forms).forEach(([domain, roles]) => {
        State.situationMap.set(domain, roles);
      });
      console.log(`✓ Loaded ${State.situationMap.size} situation definitions`);
    }
  } catch (err) {
    console.error("API connection failed:", err);
    updateStatus(apiText, "Offline", "#ef4444");
    if (apiDot) apiDot.classList.remove('online');
  }
}

async function handleLookup() {
  const verbInput = getElement('verbInput');
  const status = getElement('verbStatus');
  const senseSelect = getElement('senseSelect');
  
  if (!verbInput || !status) return;
  
  const verb = verbInput.value.trim().toLowerCase();
  if (!verb) {
    showStatus(status, "Please enter a verb", "#f59e0b");
    return;
  }
  
  State.currentVerb = verb;
  showStatus(status, "Searching...", "#94a3b8");

  try {
    const response = await fetch(`${CONFIG.API_BASE_URL}/lookup?verb=${encodeURIComponent(verb)}`);
    const data = await response.json();
    
    if (data.found) {
      showStatus(status, "✓ Lemma Found", "#10b981");
      State.currentSenses = data.senses;
      populateSenseSelect(senseSelect, data.senses);
      showElement('step2');
      
      // Auto-select if only one sense
      if (data.senses.length === 1) {
        senseSelect.value = 0;
        handleSenseSelect();
      }
    } else {
      showStatus(status, "Lemma not found", "#f59e0b");
      resetUI();
    }
  } catch (err) {
    console.error("Lookup error:", err);
    showStatus(status, "API Error", "#ef4444");
  }
}

async function runInference() {
  const btn = getElement('inferBtn');
  const ttlArea = getElement('ttlInput');
  
  if (!btn || !ttlArea) return;
  
  const originalText = btn.textContent;
  setButtonState(btn, "Running...", true);

  try {
    const turtle = PREFIXES + ttlArea.value.replace(PREFIXES, '').trimStart();
    const response = await fetch(`${CONFIG.API_BASE_URL}/infer`, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: turtle
    });
    
    const result = await response.json();
    
    if (result.success && result.inferred_data) {
      ttlArea.value = result.inferred_data;
      updateGraph();
      
      const stats = result.stats;
      console.log(`✓ Inference: ${stats.inferred_triples} new triples`);
      
      setButtonState(btn, `✓ ${stats.inferred_triples} triples`, false);
      setTimeout(() => setButtonState(btn, originalText, false), 2000);
      
      showInferenceReport(stats);
    } else {
      setButtonState(btn, "✗ Failed", false);
      setTimeout(() => setButtonState(btn, originalText, false), 2000);
    }
  } catch (err) {
    console.error("Inference failed:", err);
    setButtonState(btn, "✗ Error", false);
    setTimeout(() => setButtonState(btn, originalText, false), 2000);
  }
}

async function validateGraph() {
  const btn = getElement('validateBtn');
  const ttlArea = getElement('ttlInput');
  
  if (!btn || !ttlArea) return;
  
  const originalText = btn.textContent;
  setButtonState(btn, "Checking...", true);
  
  try {
    const turtle = PREFIXES + ttlArea.value.replace(PREFIXES, '').trimStart();
    const response = await fetch(`${CONFIG.API_BASE_URL}/infer`, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: turtle
    });
    
    const result = await response.json();
    showValidationReport(result);
    
    setButtonState(btn, result.conforms ? "✓ Valid" : "✗ Invalid", false);
    setTimeout(() => setButtonState(btn, originalText, false), 2000);
    
  } catch (err) {
    console.error("Validation failed:", err);
    setButtonState(btn, "✗ Error", false);
    setTimeout(() => setButtonState(btn, originalText, false), 2000);
  }
}

// ============================================
// UI HANDLERS
// ============================================
function handleSenseSelect() {
  const senseSelect = getElement('senseSelect');
  if (!senseSelect) return;
  
  const selectedIndex = senseSelect.value;
  if (selectedIndex === "") return;

  const sense = State.currentSenses[selectedIndex];
  if (!sense) return;

  const sitSelect = getElement('situationSelect');
  if (!sitSelect) return;
  
  sitSelect.innerHTML = '';

  if (sense.situations && sense.situations.length > 0) {
    sense.situations.forEach(sitID => {
      const label = sitID.replace(/_/g, ' ').replace(' shape', '');
      const option = document.createElement('option');
      option.value = sitID;
      option.textContent = label;
      sitSelect.appendChild(option);
    });

    showElement('step3');

    // Auto-select if only one situation
    if (sense.situations.length === 1) {
      sitSelect.value = sense.situations[0];
    }
    handleSituationSelect();
  } else {
    hideElement('step3');
  }
}

function handleSituationSelect() {
  const sitSelect = getElement('situationSelect');
  if (!sitSelect) return;
  
  const selectedShapeID = sitSelect.value;
  if (!selectedShapeID) return;
  
  State.selectedSituation = selectedShapeID;
  const shapeData = State.situationMap.get(selectedShapeID);
  
  const hint = getElement('domainHint');
  if (hint) {
    hint.textContent = shapeData 
      ? `Shape: ${selectedShapeID}` 
      : "No SHACL definition found";
  }
    
  renderForm(shapeData ? shapeData.fields : null);
}

function renderForm(fields) {
  const form = getElement('dynamicForm');
  if (!form) return;
  
  form.innerHTML = '';
  
  if (!fields) {
    hideElement('stepForm');
    return;
  }
  
  fields.forEach(field => {
    const row = createRoleRow(field);
    form.appendChild(row);
  });
  
  showElement('stepForm');
}

function createRoleRow(field) {
  const row = document.createElement('div');
  row.className = 'role-row';
  
  row.innerHTML = `
    <label class="role-label">${field.label}${field.required ? ' *' : ''}</label>
    <div class="compact-row">
      <select class="type-select">
        <option value="Entity">Entity</option>
        <option value="Instance">Instance</option>
        <option value="Literal">Literal</option>
        <option value="IRI">IRI</option>
        <option value="BNode">_:</option>
      </select>
      <input class="role-input" type="text" 
             data-role="${field.label}" 
             data-path="${field.path}" 
             placeholder="Value..." 
             ${field.required ? 'required' : ''}>
      <select class="instance-select hidden" style="flex: 1;">
        <option value="">-- Select Instance --</option>
      </select>
    </div>
  `;
  
  // Setup type switching
  const typeSelect = row.querySelector('.type-select');
  const textInput = row.querySelector('.role-input');
  const instanceSelect = row.querySelector('.instance-select');
  
  typeSelect.addEventListener('change', () => {
    if (typeSelect.value === 'Instance') {
      textInput.classList.add('hidden');
      instanceSelect.classList.remove('hidden');
      populateInstanceSelect(instanceSelect);
    } else {
      textInput.classList.remove('hidden');
      instanceSelect.classList.add('hidden');
    }
  });
  
  return row;
}

function populateInstanceSelect(selectElement) {
  const ttl = getElement('ttlInput')?.value || '';
  const instances = [];
  
  // Parse existing temp: instances
  const pattern = /temp:s(\d+)\s+a\s+:(\w+)\s*;[\s\S]*?rdfs:label\s+"([^"]+)"/g;
  let match;
  
  while ((match = pattern.exec(ttl)) !== null) {
    instances.push({
      id: `temp:s${match[1]}`,
      className: match[2],
      label: match[3]
    });
  }
  
  // Populate select
  selectElement.innerHTML = '<option value="">-- Select Instance --</option>';
  
  if (instances.length === 0) {
    selectElement.innerHTML += '<option value="" disabled>-- No instances yet --</option>';
  } else {
    instances.forEach(inst => {
      const option = document.createElement('option');
      option.value = inst.id;
      option.textContent = `${inst.label} (${inst.className})`;
      selectElement.appendChild(option);
    });
  }
}

async function addEntry() {
  State.globalCount++;
  const sitId = `temp:s${State.globalCount}`;
  const ttlArea = getElement('ttlInput');
  
  if (!ttlArea) return;
  
  // Get synset
  const selectedIndex = getElement('senseSelect')?.value;
  const sense = State.currentSenses[selectedIndex];
  
  // Build main block
  const className = State.selectedSituation.replace('_shape', '');
  let mainBlock = `${sitId} a :${className} ;\n    rdfs:label "${State.currentVerb}" ;\n    :lemma "${State.currentVerb}"`;
  
  if (sense && sense.gloss) {
    mainBlock += ` ;\n    :synset "${sense.gloss}"`;
  }
  
  let entityBlock = "";

  // Process roles
  document.querySelectorAll('.role-row').forEach(row => {
    const type = row.querySelector('.type-select').value;
    const input = row.querySelector('.role-input');
    const instanceSelect = row.querySelector('.instance-select');
    const value = type === 'Instance' ? instanceSelect.value : input.value.trim();
    
    if (!value) return;
    
    let predicate = `:${input.dataset.role}`;
    if (input.dataset.path && input.dataset.path !== 'unknown') {
      const parts = input.dataset.path.split(/[#/]/);
      predicate = `:${parts[parts.length - 1]}`;
    }
    
    const triple = buildTriple(type, value, predicate, ttlArea.value);
    mainBlock += triple.main;
    entityBlock += triple.entity;
  });

  const newData = `${mainBlock} .\n${entityBlock}\n`;
  ttlArea.value += newData;
  
  updateGraph();
  showSuccess('addEntryBtn', "✓ Added");
}

function buildTriple(type, value, predicate, existingTTL) {
  const result = { main: '', entity: '' };
  
  switch (type) {
    case 'Instance':
      result.main = ` ;\n    ${predicate} ${value}`;
      break;
      
    case 'Entity':
      const slug = toSlug(value);
      const tempIRI = `temp:${slug}`;
      result.main = ` ;\n    ${predicate} ${tempIRI}`;
      if (!existingTTL.includes(`${tempIRI} a :Entity`)) {
        result.entity = `${tempIRI} a :Entity ;\n    rdfs:label "${value}" .\n`;
      }
      break;
      
    case 'Literal':
      result.main = ` ;\n    ${predicate} "${value}"`;
      break;
      
    case 'IRI':
      const iri = value.startsWith('<') ? value : 
                  value.includes(':') ? value : `:${value}`;
      result.main = ` ;\n    ${predicate} ${iri}`;
      break;
      
    case 'BNode':
      const bnode = value.startsWith('_:') ? value : `_:${value}`;
      result.main = ` ;\n    ${predicate} ${bnode}`;
      break;
  }
  
  return result;
}

// ============================================
// REPORT DISPLAY
// ============================================
function showInferenceReport(stats) {
  const reportBox = getElement('validationReport');
  const reportHeader = reportBox?.querySelector('.report-header');
  const reportContent = getElement('reportContent');
  
  if (!reportBox || !reportHeader || !reportContent) return;
  
  const success = stats.inferred_triples > 0;
  
  reportHeader.textContent = success ? "✓ Inference Complete" : "⚠ No Triples Inferred";
  reportHeader.style.background = success ? "#1e40af" : "#854d0e";
  reportHeader.style.color = success ? "#93c5fd" : "#fde047";
  
  let message = `Asserted triples: ${stats.input_triples}
Inferred triples: ${stats.inferred_triples}
Total triples: ${stats.total_triples}`;

  if (success) {
    const plural = stats.inferred_triples === 1 ? 'property' : 'properties';
    message += `\n\n✓ Successfully generated ${stats.inferred_triples} opaque ${plural}`;
  } else {
    message += `\n\n⚠ No opaque properties were generated.
Check that your data has :lemma and :synset properties.`;
  }
  
  reportContent.textContent = message;
  reportContent.style.color = success ? "#93c5fd" : "#fde047";
  reportBox.classList.remove('hidden');
  reportBox.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function showValidationReport(result) {
  const reportBox = getElement('validationReport');
  const reportHeader = reportBox?.querySelector('.report-header');
  const reportContent = getElement('reportContent');
  
  if (!reportBox || !reportHeader || !reportContent) return;
  
  reportHeader.textContent = result.conforms ? "✓ Validation Passed" : "✗ Validation Failed";
  reportHeader.style.background = result.conforms ? "#065f46" : "#422006";
  reportHeader.style.color = result.conforms ? "#6ee7b7" : "#fcd34d";
  
  reportContent.textContent = result.report_text;
  reportContent.style.color = result.conforms ? "#6ee7b7" : "#fcd34d";
  reportBox.classList.remove('hidden');
  reportBox.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ============================================
// GRAPH VISUALIZATION
// ============================================
function initGraph() {
  if (typeof d3 === "undefined") {
    console.warn("D3 not loaded during initGraph");
    return;
  }

  const container = getElement('graph-container');
  if (!container) return;

  // Clear existing
  d3.select("#graph-container").selectAll("*").remove();

  // Create SVG
  State.svg = d3.select("#graph-container")
    .append("svg")
    .attr("width", "100%")
    .attr("height", "100%");

  // Setup zoom
  State.zoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on("zoom", (event) => {
      if (State.g) State.g.attr("transform", event.transform);
    });
  
  State.svg.call(State.zoom);

  // Add arrow marker
  State.svg.append("defs")
    .append("marker")
    .attr("id", "arrowhead")
    .attr("viewBox", "-0 -5 10 10")
    .attr("refX", 20)
    .attr("refY", 0)
    .attr("orient", "auto")
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .append("svg:path")
    .attr("d", "M 0,-5 L 10 ,0 L 0,5")
    .attr("fill", "#334155");

  // Create main group
  State.g = State.svg.append("g");

  // Create force simulation
  State.simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(d => d.id).distance(CONFIG.GRAPH.LINK_DISTANCE))
    .force("charge", d3.forceManyBody().strength(CONFIG.GRAPH.CHARGE_STRENGTH))
    .force("center", d3.forceCenter(container.clientWidth / 2, container.clientHeight / 2))
    .force("collide", d3.forceCollide(CONFIG.GRAPH.COLLIDE_RADIUS));
  
  console.log("✓ Graph initialized");
}

function updateGraph() {
  if (!State.g) {
    initGraph();
    if (!State.g) return;
  }
  
  const ttlInput = getElement('ttlInput');
  if (!ttlInput) return;
  
  const { nodes, links } = parseTTL(ttlInput.value);
  renderGraph(nodes, links);
}

function parseTTL(rawText) {
  const nodes = [];
  const links = [];
  const nodeMap = new Map();
  let bnodeCount = 0;
  let currentSubject = null;

  const getOrCreateNode = (id, label, group) => {
    if (!nodeMap.has(id)) {
      const node = {
        id,
        label: label || id,
        group,
        r: CONFIG.GRAPH.NODE_SIZES[group] || 16
      };
      nodeMap.set(id, node);
      nodes.push(node);
    }
    return nodeMap.get(id);
  };

  rawText.split('\n').forEach(line => {
    line = line.trim();
    if (!line || line.startsWith('@prefix')) return;
    
    const cleanContent = line.replace(/[;.,\]]+$/, '').trim();

    // Handle blank nodes
    if (line.startsWith('[')) {
      bnodeCount++;
      currentSubject = `_:bnode${bnodeCount}`;
      const typeMatch = cleanContent.match(/(?:a|rdf:type)\s+(\S+)/);
      if (typeMatch) {
        const type = typeMatch[1].split(':').pop();
        getOrCreateNode(currentSubject, "Situation", 'instance');
        const classId = `Class:${type}`;
        getOrCreateNode(classId, type, 'class');
        links.push({
          source: currentSubject,
          target: classId,
          label: 'type',
          isInferred: false
        });
      }
      return;
    }

    // Handle subject declarations
    const subjMatch = cleanContent.match(/^(\S+)\s+(?:a|rdf:type)\s+(\S+)/);
    if (subjMatch && !line.startsWith(']')) {
      currentSubject = subjMatch[1];
      const type = subjMatch[2].split(':').pop().replace(/[<>]/g, '');
      const label = currentSubject.startsWith('temp:') 
        ? currentSubject.slice(5).replace(/_/g, ' ') 
        : currentSubject;
      
      getOrCreateNode(currentSubject, label, 'instance');
      const classId = `Class:${type}`;
      getOrCreateNode(classId, type, 'class');
      links.push({
        source: currentSubject,
        target: classId,
        label: 'type',
        isInferred: false
      });
      return;
    }

    // Handle properties
    if (currentSubject && !line.startsWith(']')) {
      const propMatch = cleanContent.match(/^(\S+)\s+(.+)/);
      if (propMatch) {
        const [_, pred, val] = propMatch;
        const predLabel = pred.split(/[/#:]/).pop();
        
        // Update node label if this is rdfs:label
        if (pred === 'rdfs:label' || pred === 'label') {
          const node = nodeMap.get(currentSubject);
          if (node) node.label = val.replace(/"/g, '');
        } 
        // Skip type and metadata predicates
        else if (!['rdf:type', 'a', ':lemma', ':synset'].includes(pred)) {
          const isInferred = predLabel.includes('_') && 
                            predLabel.match(/_[a-f0-9]{12}$/);
          
          if (val.startsWith('"')) {
            // Literal value
            const litVal = val.replace(/"/g, '');
            const litId = `Lit:${litVal}_${currentSubject}`;
            getOrCreateNode(litId, `"${litVal}"`, 'literal');
            links.push({
              source: currentSubject,
              target: litId,
              label: predLabel,
              isInferred
            });
          } else {
            // Object reference
            const targetLabel = val.startsWith('temp:') 
              ? val.slice(5).replace(/_/g, ' ') 
              : val;
            getOrCreateNode(val, targetLabel, 'instance');
            links.push({
              source: currentSubject,
              target: val,
              label: predLabel,
              isInferred
            });
          }
        }
      }
    }

    // Reset subject at end of block
    if (line.endsWith('] .') || line.startsWith(']')) {
      currentSubject = null;
    }
  });

  return { nodes, links };
}

function renderGraph(nodes, links) {
  if (!State.g) return;
  
  State.g.selectAll("*").remove();

  // Create links
  const link = State.g.append("g")
    .selectAll("line")
    .data(links)
    .enter()
    .append("line")
    .attr("class", d => d.isInferred ? "link inferred" : "link")
    .attr("marker-end", "url(#arrowhead)");

  // Create link labels
  const linkLabel = State.g.append("g")
    .selectAll("g")
    .data(links)
    .enter()
    .append("g");
  
  linkLabel.append("rect")
    .attr("rx", 3)
    .attr("ry", 3)
    .attr("fill", "#020617")
    .attr("opacity", 0.9);
  
  linkLabel.append("text")
    .attr("class", "link-label-text")
    .attr("dy", 3)
    .text(d => d.label);

  // Create nodes
  const node = State.g.append("g")
    .selectAll("g")
    .data(nodes)
    .enter()
    .append("g")
    .call(d3.drag()
      .on("start", dragstarted)
      .on("drag", dragged)
      .on("end", dragended));

  node.append("circle")
    .attr("r", d => d.r)
    .attr("fill", d => CONFIG.COLORS[d.group])
    .attr("stroke", "#fff")
    .attr("stroke-width", 1);

  // Create node labels
  const nodeLabel = node.append("g");
  
  nodeLabel.append("rect")
    .attr("rx", 3)
    .attr("ry", 3)
    .attr("fill", "#020617")
    .attr("opacity", 0.85);
  
  nodeLabel.append("text")
    .attr("class", "node-label-text")
    .attr("dy", -25)
    .attr("text-anchor", "middle")
    .text(d => d.label);

  // Tick function
  State.simulation.nodes(nodes).on("tick", () => {
    link
      .attr("x1", d => d.source.x)
      .attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x)
      .attr("y2", d => d.target.y);

    linkLabel.attr("transform", d => 
      `translate(${(d.source.x + d.target.x) / 2}, ${(d.source.y + d.target.y) / 2})`
    );

    linkLabel.each(function() {
      const bbox = d3.select(this).select("text").node().getBBox();
      d3.select(this).select("rect")
        .attr("x", bbox.x - 4)
        .attr("y", bbox.y - 2)
        .attr("width", bbox.width + 8)
        .attr("height", bbox.height + 4);
    });

    node.attr("transform", d => `translate(${d.x},${d.y})`);

    node.selectAll("g").each(function() {
      const bbox = d3.select(this).select("text").node().getBBox();
      d3.select(this).select("rect")
        .attr("x", bbox.x - 4)
        .attr("y", bbox.y - 2)
        .attr("width", bbox.width + 8)
        .attr("height", bbox.height + 4);
    });
  });

  // Enable Save button if there is meaningful content
  const saveBtn = getElement('saveBtn');
  if (saveBtn) {
    const hasMeaningfulContent = (getElement('ttlInput')?.value || '').replace(PREFIXES, '').trim().length > 0;
    saveBtn.disabled = !hasMeaningfulContent;
  }

  State.simulation.force("link").links(links);
  State.simulation.alpha(1).restart();
  
  // Auto zoom to fit
  setTimeout(zoomToFit, 150);
}

function zoomToFit() {
  if (!State.g || !State.svg) return;
  
  const bounds = State.g.node().getBBox();
  const parent = State.svg.node().parentElement;
  
  if (bounds.width === 0 || bounds.height === 0) return;
  
  const midX = bounds.x + bounds.width / 2;
  const midY = bounds.y + bounds.height / 2;
  const scale = 0.75 / Math.max(
    bounds.width / parent.clientWidth,
    bounds.height / parent.clientHeight
  );
  
  State.svg.transition()
    .duration(750)
    .call(
      State.zoom.transform,
      d3.zoomIdentity
        .translate(
          parent.clientWidth / 2 - scale * midX,
          parent.clientHeight / 2 - scale * midY
        )
        .scale(scale)
    );
}

function handleResize() {
  const container = getElement('graph-container');
  if (container && State.simulation) {
    State.simulation.force("center", 
      d3.forceCenter(container.clientWidth / 2, container.clientHeight / 2)
    );
  }
}

function dragstarted(event, d) {
  if (!event.active) State.simulation.alphaTarget(0.3).restart();
  d.fx = d.x;
  d.fy = d.y;
}

function dragged(event, d) {
  d.fx = event.x;
  d.fy = event.y;
}

function dragended(event, d) {
  if (!event.active) State.simulation.alphaTarget(0);
  d.fx = null;
  d.fy = null;
}

// ============================================
// UTILITY FUNCTIONS
// ============================================
function getElement(id) {
  return document.getElementById(id);
}

function showElement(id) {
  const el = getElement(id);
  if (el) el.classList.remove('hidden');
}

function hideElement(id) {
  const el = getElement(id);
  if (el) el.classList.add('hidden');
}

function showStatus(element, text, color) {
  if (element) {
    element.textContent = text;
    element.style.color = color;
  }
}

function updateStatus(element, text, color) {
  if (element) {
    element.textContent = text;
    if (color) element.style.color = color;
  }
}

function setButtonState(button, text, disabled) {
  if (button) {
    button.textContent = text;
    button.disabled = disabled;
  }
}

function showSuccess(buttonId, text, duration = 1000) {
  const btn = getElement(buttonId);
  if (!btn) return;
  
  const originalText = btn.textContent;
  btn.textContent = text;
  setTimeout(() => { btn.textContent = originalText; }, duration);
}

function showError(message) {
  console.error(message);
  // Could add a toast notification here
}

function toSlug(value) {
  return value.trim()
    .replace(/\s+/g, '_')
    .replace(/[<>"{}|\\^`]/g, '');
}

function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

function populateSenseSelect(selectElement, senses) {
  if (!selectElement) return;
  
  selectElement.innerHTML = '<option value="">-- Select a Meaning --</option>';
  
  senses.forEach((sense, i) => {
    const option = document.createElement('option');
    option.value = i;
    const shortGloss = sense.gloss.length > 60 
      ? sense.gloss.substring(0, 60) + "..." 
      : sense.gloss;
    option.textContent = `${sense.id}: ${shortGloss}`;
    selectElement.appendChild(option);
  });
}

// ============================================
// FILE OPERATIONS
// ============================================
function handleNewGraph() {
  const ttlInput = getElement('ttlInput');
  if (ttlInput) {
    ttlInput.value = PREFIXES;
    updateGraph();
  }
  resetUI();
}

function resetUI() {
  ['step2', 'step3', 'stepForm', 'validationReport'].forEach(hideElement);
  State.reset();
}

function downloadTTL() {
  const ttlInput = getElement('ttlInput');
  if (!ttlInput) return;
  
  const blob = new Blob([ttlInput.value], { type: 'text/turtle' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = "data.ttl";
  link.click();
  URL.revokeObjectURL(url);
}

function handleDownloadTemplate() {
  const fields = Array.from(document.querySelectorAll('.role-row input'))
    .map(input => input.dataset.role);
  
  const csvContent = [
    ['Verb', 'Situation', ...fields].join(','),
    [State.currentVerb, State.selectedSituation, ...fields.map(() => '')].join(',')
  ].join('\n');
  
  const blob = new Blob([csvContent], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = "template.csv";
  link.click();
  URL.revokeObjectURL(url);
}

// ============================================
// START APPLICATION
// ============================================
checkDependencies();

// ============================================
// SAVE TO GRAPH DB
// ============================================
async function saveToGraph() {
  const btn = getElement('saveBtn');
  const ttlArea = getElement('ttlInput');

  if (!btn || !ttlArea) return;

  const turtle = ttlArea.value;
  const hasMeaningfulContent = turtle.replace(PREFIXES, '').trim().length > 0;
  if (!hasMeaningfulContent) return;

  const originalText = btn.textContent;
  setButtonState(btn, '⏳ Saving…', true);

  try {
    const res = await fetch(`${CONFIG.API_BASE_URL}/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      body: turtle
    });

    if (res.ok) {
      const data = await res.json();
      setButtonState(btn, `✅ Saved (${data.tripleCount} triples)`, false);
      setTimeout(() => setButtonState(btn, originalText, false), 3000);
    } else {
      const msg = await res.text();
      console.error('Save error:', msg);
      setButtonState(btn, '❌ Save failed', false);
      setTimeout(() => setButtonState(btn, originalText, false), 3000);
    }
  } catch (err) {
    console.error('Save network error:', err);
    setButtonState(btn, '❌ Network error', false);
    setTimeout(() => setButtonState(btn, originalText, false), 3000);
  }
}
