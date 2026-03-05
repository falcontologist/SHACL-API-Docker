// ============================================
// CONFIGURATION & CONSTANTS
// ============================================
const CONFIG = {
  API_BASE_URL: (window.location.hostname === 'localhost' || 
                window.location.hostname === '127.0.0.1' ||
                window.location.hostname === '')
    ? "http://localhost:8080/api"
    : "https://shacl-api-docker.onrender.com/api",
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
  },
  ENTITY_CATEGORIES: ["Person_Entity", "Organization_Entity", "Geopolitical_Entity", "Product_Entity", "Unit_Entity", "Occupation_Entity"],
  SUGGEST_DEBOUNCE_MS: 150,
  SUGGEST_MIN_CHARS: 2,
  SUGGEST_MAX_RESULTS: 10
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
  
  // Entity suggest state (per-row, keyed by row index)
  entitySuggestControllers: new Map(), // AbortControllers for in-flight requests
  
  // Graph state
  simulation: null,
  svg: null,
  g: null,
  zoom: null,
  
  reset() {
    this.currentSenses = [];
    this.selectedSituation = null;
    this.currentVerb = "";
    // Abort any in-flight suggest requests
    this.entitySuggestControllers.forEach(c => c.abort());
    this.entitySuggestControllers.clear();
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
  
  // Close all suggest dropdowns when clicking outside
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.entity-suggest-wrapper')) {
      document.querySelectorAll('.suggest-dropdown').forEach(d => d.classList.add('hidden'));
    }
  });
  
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
    ttlInput.addEventListener('input', () => {
      const saveBtn = getElement('saveBtn');
      if (saveBtn) saveBtn.disabled = ttlInput.value.replace(/^@prefix[^\n]*\n/gm, '').trim().length === 0;
    });
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
// ENTITY SUGGEST API
// ============================================

/**
 * Query the entity suggest endpoint.
 * Returns {results: [{label, iri, category, matchedLabel?}], count, latencyMicros}
 */
async function fetchEntitySuggestions(category, query, signal) {
  const url = `${CONFIG.API_BASE_URL}/entity-suggest?type=${encodeURIComponent(category)}&q=${encodeURIComponent(query)}&limit=${CONFIG.SUGGEST_MAX_RESULTS}`;
  const resp = await fetch(url, { signal });
  if (!resp.ok) throw new Error(`Suggest returned ${resp.status}`);
  return resp.json();
}

/**
 * Fetch senses for a given entity IRI.
 * Returns {iri, senses: [{senseId, senseIRI, gloss, label}], count}
 */
async function fetchEntitySenses(iri) {
  const url = `${CONFIG.API_BASE_URL}/entity-senses?iri=${encodeURIComponent(iri)}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Entity senses returned ${resp.status}`);
  return resp.json();
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
  
  fields.forEach((field, index) => {
    const row = createRoleRow(field, index);
    form.appendChild(row);
  });
  
  showElement('stepForm');
}

// ============================================
// ENTITY AUTOCOMPLETE ROW BUILDER
// ============================================

/**
 * Creates a form row for a SHACL property slot.
 * When "Entity" is selected as the type, shows:
 *   [Category dropdown] [Search input with autocomplete] [Sense dropdown (after selection)]
 */
function createRoleRow(field, rowIndex) {
  const row = document.createElement('div');
  row.className = 'role-row';
  row.dataset.rowIndex = rowIndex;
  
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

      <!-- Entity mode: category + autocomplete + sense selector -->
      <div class="entity-suggest-wrapper">
        <select class="entity-category-select">
          ${CONFIG.ENTITY_CATEGORIES.map(c => 
            `<option value="${c}">${formatCategoryLabel(c)}</option>`
          ).join('')}
        </select>
        <div class="suggest-input-container">
          <input class="entity-search-input" type="text" 
                 placeholder="Search entities..." 
                 autocomplete="off"
                 data-role="${field.label}" 
                 data-path="${field.path}">
          <div class="suggest-spinner hidden"></div>
          <div class="suggest-dropdown hidden"></div>
        </div>
        <select class="entity-sense-select hidden">
          <option value="">-- Select Sense --</option>
        </select>
      </div>

      <!-- Plain text input (for Literal, IRI, BNode modes) -->
      <input class="role-input hidden" type="text" 
             data-role="${field.label}" 
             data-path="${field.path}" 
             placeholder="Value..." 
             ${field.required ? 'required' : ''}>

      <!-- Instance reference select -->
      <select class="instance-select hidden" style="flex: 1;">
        <option value="">-- Select Instance --</option>
      </select>
    </div>
  `;
  
  // --- Wire up type switching ---
  const typeSelect = row.querySelector('.type-select');
  const entityWrapper = row.querySelector('.entity-suggest-wrapper');
  const textInput = row.querySelector('.role-input');
  const instanceSelect = row.querySelector('.instance-select');
  
  typeSelect.addEventListener('change', () => {
    const val = typeSelect.value;
    
    // Hide all first
    entityWrapper.classList.add('hidden');
    textInput.classList.add('hidden');
    instanceSelect.classList.add('hidden');
    
    if (val === 'Entity') {
      entityWrapper.classList.remove('hidden');
    } else if (val === 'Instance') {
      instanceSelect.classList.remove('hidden');
      populateInstanceSelect(instanceSelect);
    } else {
      textInput.classList.remove('hidden');
    }
  });

  // Default: show entity wrapper
  entityWrapper.classList.remove('hidden');
  textInput.classList.add('hidden');
  instanceSelect.classList.add('hidden');

  // --- Wire up entity autocomplete ---
  const searchInput = row.querySelector('.entity-search-input');
  const categorySelect = row.querySelector('.entity-category-select');
  const dropdown = row.querySelector('.suggest-dropdown');
  const spinner = row.querySelector('.suggest-spinner');
  const senseSelect = row.querySelector('.entity-sense-select');

  // Debounced suggest handler
  const debouncedSuggest = debounce(async () => {
    const query = searchInput.value.trim();
    const category = categorySelect.value;
    
    if (query.length < CONFIG.SUGGEST_MIN_CHARS) {
      dropdown.classList.add('hidden');
      return;
    }

    // Abort previous request for this row
    const prevController = State.entitySuggestControllers.get(rowIndex);
    if (prevController) prevController.abort();
    
    const controller = new AbortController();
    State.entitySuggestControllers.set(rowIndex, controller);

    spinner.classList.remove('hidden');

    try {
      const data = await fetchEntitySuggestions(category, query, controller.signal);
      renderSuggestDropdown(dropdown, data.results, searchInput, senseSelect, row);
      spinner.classList.add('hidden');
    } catch (err) {
      if (err.name !== 'AbortError') {
        console.error('Suggest error:', err);
        spinner.classList.add('hidden');
      }
    }
  }, CONFIG.SUGGEST_DEBOUNCE_MS);

  searchInput.addEventListener('input', debouncedSuggest);

  // Keyboard navigation for dropdown
  searchInput.addEventListener('keydown', (e) => {
    if (dropdown.classList.contains('hidden')) return;
    const items = dropdown.querySelectorAll('.suggest-item');
    const active = dropdown.querySelector('.suggest-item.active');
    let idx = Array.from(items).indexOf(active);

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (idx < items.length - 1) idx++;
      else idx = 0;
      items.forEach(i => i.classList.remove('active'));
      items[idx]?.classList.add('active');
      items[idx]?.scrollIntoView({ block: 'nearest' });
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (idx > 0) idx--;
      else idx = items.length - 1;
      items.forEach(i => i.classList.remove('active'));
      items[idx]?.classList.add('active');
      items[idx]?.scrollIntoView({ block: 'nearest' });
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (active) active.click();
    } else if (e.key === 'Escape') {
      dropdown.classList.add('hidden');
    }
  });

  // Close dropdown on blur (with delay for click to register)
  searchInput.addEventListener('blur', () => {
    setTimeout(() => dropdown.classList.add('hidden'), 200);
  });
  
  // Re-query when category changes
  categorySelect.addEventListener('change', () => {
    searchInput.value = '';
    senseSelect.classList.add('hidden');
    senseSelect.innerHTML = '<option value="">-- Select Sense --</option>';
    dropdown.classList.add('hidden');
    // Clear resolved entity data
    delete searchInput.dataset.resolvedIri;
    delete searchInput.dataset.resolvedLabel;
    searchInput.focus();
  });

  return row;
}

/**
 * Render suggest dropdown with results.
 */
function renderSuggestDropdown(dropdown, results, searchInput, senseSelect, row) {
  dropdown.innerHTML = '';

  if (results.length === 0) {
    dropdown.innerHTML = '<div class="suggest-empty">No matches found</div>';
    dropdown.classList.remove('hidden');
    return;
  }

  results.forEach((item, i) => {
    const div = document.createElement('div');
    div.className = 'suggest-item' + (i === 0 ? ' active' : '');
    
    const labelSpan = document.createElement('span');
    labelSpan.className = 'suggest-label';
    labelSpan.textContent = item.label;
    div.appendChild(labelSpan);

    if (item.matchedLabel && item.matchedLabel !== item.label) {
      const altSpan = document.createElement('span');
      altSpan.className = 'suggest-alt';
      altSpan.textContent = `(${item.matchedLabel})`;
      div.appendChild(altSpan);
    }

    const iriSpan = document.createElement('span');
    iriSpan.className = 'suggest-iri';
    // Show just the local name from the IRI
    const localName = item.iri.includes('/') 
      ? item.iri.substring(item.iri.lastIndexOf('/') + 1) 
      : item.iri.includes('#')
        ? item.iri.substring(item.iri.lastIndexOf('#') + 1)
        : item.iri;
    iriSpan.textContent = decodeURIComponent(localName);
    div.appendChild(iriSpan);

    if (item.gloss) {
      const glossSpan = document.createElement('span');
      glossSpan.className = 'suggest-gloss';
      glossSpan.textContent = item.gloss.length > 80 
        ? item.gloss.substring(0, 80) + '...' 
        : item.gloss;
      div.appendChild(glossSpan);
    }

    div.addEventListener('click', () => {
      handleEntitySelection(item, searchInput, senseSelect, dropdown, row);
    });

    dropdown.appendChild(div);
  });

  dropdown.classList.remove('hidden');
}

/**
 * Handle entity selection: set input, fetch senses, resolve IRI.
 */
async function handleEntitySelection(item, searchInput, senseSelect, dropdown, row) {
  searchInput.value = item.label;
  searchInput.dataset.resolvedIri = item.iri;
  searchInput.dataset.resolvedLabel = item.label;
  dropdown.classList.add('hidden');

  // Fetch senses for this entity
  try {
    const data = await fetchEntitySenses(item.iri);
    
    if (data.senses && data.senses.length > 0) {
      senseSelect.innerHTML = '<option value="">-- Select Sense --</option>';
      data.senses.forEach(sense => {
        const opt = document.createElement('option');
        opt.value = sense.senseIRI;
        // Show label + gloss, or just senseId if no gloss
        const displayLabel = sense.label || sense.senseId;
        const gloss = sense.gloss && sense.gloss.trim() 
          ? sense.gloss.trim() : '';
        if (gloss) {
          const shortGloss = gloss.length > 60 
            ? gloss.substring(0, 60) + '...' 
            : gloss;
          opt.textContent = `${displayLabel} — ${shortGloss}`;
        } else {
          opt.textContent = displayLabel;
        }
        opt.dataset.senseIri = sense.senseIRI;
        senseSelect.appendChild(opt);
      });
      senseSelect.classList.remove('hidden');

      // Auto-select if only one sense
      if (data.senses.length === 1) {
        senseSelect.value = data.senses[0].senseIRI;
      }
    } else {
      // No senses — just use the IRI directly
      senseSelect.classList.add('hidden');
    }
  } catch (err) {
    console.error('Failed to fetch entity senses:', err);
    senseSelect.classList.add('hidden');
  }
}

function formatCategoryLabel(category) {
  switch (category) {
    case 'Geopolitical_Entity': return 'Geo-Political';
    case 'Organization_Entity': return 'Organization';
    case 'Person_Entity': return 'Person';
    case 'Product_Entity': return 'Product';
    case 'Unit_Entity': return 'Unit';
    case 'Occupation_Entity': return 'Occupation';
    default: return category.replace(/_Entity$/, '');
  }
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

// ============================================
// ADD ENTRY (updated for Entity mode)
// ============================================
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
    const textInput = row.querySelector('.role-input');
    const instanceSelect = row.querySelector('.instance-select');
    const entitySearchInput = row.querySelector('.entity-search-input');
    const entitySenseSelect = row.querySelector('.entity-sense-select');
    
    let value = '';
    let resolvedIri = null;
    
    if (type === 'Entity') {
      // Entity mode: use resolved IRI if available
      resolvedIri = entitySearchInput?.dataset.resolvedIri;
      value = entitySearchInput?.value.trim() || '';
      
      if (!value) return;
    } else if (type === 'Instance') {
      value = instanceSelect.value;
    } else {
      value = textInput.value.trim();
    }
    
    if (!value) return;
    
    // Determine predicate
    const dataInput = type === 'Entity' ? entitySearchInput : textInput;
    let predicate = `:${dataInput.dataset.role}`;
    if (dataInput.dataset.path && dataInput.dataset.path !== 'unknown') {
      const parts = dataInput.dataset.path.split(/[#/]/);
      predicate = `:${parts[parts.length - 1]}`;
    }
    
    if (type === 'Entity' && resolvedIri) {
      // Use the resolved IRI from the suggest service
      const iriRef = `<${resolvedIri}>`;
      mainBlock += ` ;\n    ${predicate} ${iriRef}`;
    } else {
      const triple = buildTriple(type, value, predicate, ttlArea.value);
      mainBlock += triple.main;
      entityBlock += triple.entity;
    }
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
  
  const text = ttlInput.value;
  const { nodes, links } = parseTurtle(text);
  
  // Clear
  State.g.selectAll(".link").remove();
  State.g.selectAll(".node").remove();
  State.g.selectAll(".link-label-group").remove();
  
  if (nodes.length === 0) return;

  // Create links
  const link = State.g.selectAll(".link")
    .data(links)
    .join("line")
    .attr("class", d => `link${d.inferred ? ' inferred' : ''}`)
    .attr("marker-end", "url(#arrowhead)");

  // Create link labels
  const linkLabel = State.g.selectAll(".link-label-group")
    .data(links)
    .join("g")
    .attr("class", "link-label-group");
  
  linkLabel.append("rect")
    .attr("fill", "#0f172a")
    .attr("rx", 3)
    .attr("opacity", 0.85);
  
  linkLabel.append("text")
    .attr("class", "link-label-text")
    .attr("text-anchor", "middle")
    .attr("dy", "0.35em")
    .text(d => d.label);

  // Create nodes
  const node = State.g.selectAll(".node")
    .data(nodes)
    .join("g")
    .attr("class", "node")
    .call(d3.drag()
      .on("start", dragstarted)
      .on("drag", dragged)
      .on("end", dragended));
  
  node.append("circle")
    .attr("r", d => CONFIG.GRAPH.NODE_SIZES[d.type] || 14)
    .attr("fill", d => CONFIG.COLORS[d.type] || "#888")
    .attr("stroke", "#0f172a")
    .attr("stroke-width", 2);

  // Node labels
  const labelGroup = node.append("g").attr("transform", "translate(0, 25)");
  
  labelGroup.append("rect")
    .attr("fill", "#0f172a")
    .attr("rx", 3)
    .attr("opacity", 0.85);
  
  labelGroup.append("text")
    .attr("class", "node-label-text")
    .attr("text-anchor", "middle")
    .attr("dy", "0.35em")
    .text(d => d.label);

  // Update simulation
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
    const hasMeaningfulContent = (getElement('ttlInput')?.value || '').replace(/^@prefix[^\n]*\n/gm, '').trim().length > 0;
    saveBtn.disabled = !hasMeaningfulContent;
  }

  State.simulation.force("link").links(links);
  State.simulation.alpha(1).restart();
  
  // Auto zoom to fit
  setTimeout(zoomToFit, 150);
}

function parseTurtle(text) {
  const nodes = [];
  const links = [];
  const nodeMap = new Map();
  
  const prefixes = {};
  const prefixPattern = /@prefix\s+(\w*):?\s*<([^>]+)>\s*\./g;
  let match;
  while ((match = prefixPattern.exec(text)) !== null) {
    prefixes[match[1]] = match[2];
  }
  
  const clean = text.replace(/@prefix[^\n]+\n/g, '').trim();
  if (!clean) return { nodes, links };
  
  // Split into blocks by ".\n"
  const blocks = clean.split(/\.\s*\n/).filter(b => b.trim());
  
  blocks.forEach(block => {
    const lines = block.trim().split(/\s*;\s*\n\s*/);
    if (lines.length === 0) return;
    
    const firstLine = lines[0].trim();
    const firstParts = firstLine.match(/^(\S+)\s+(\S+)\s+(.*)/s);
    if (!firstParts) return;
    
    const subject = firstParts[1];
    let subjectLabel = subject;
    
    // Find label
    const labelMatch = block.match(/rdfs:label\s+"([^"]+)"/);
    if (labelMatch) subjectLabel = labelMatch[1];
    
    // Determine type
    const typeMatch = block.match(/a\s+:(\w+)/);
    const nodeType = typeMatch ? 'instance' : 'class';
    
    if (!nodeMap.has(subject)) {
      nodeMap.set(subject, { id: subject, label: subjectLabel, type: nodeType });
      nodes.push(nodeMap.get(subject));
    }
    
    // Parse predicates
    lines.forEach((line, i) => {
      const parts = i === 0 
        ? [firstParts[2], firstParts[3]]
        : line.trim().split(/\s+(.+)/);
      
      if (!parts || parts.length < 2) return;
      
      const predicate = parts[0].trim();
      let object = parts[1].trim().replace(/\s*\.$/, '');
      
      if (predicate === 'a') return;
      if (predicate === 'rdfs:label') return;
      if (predicate === ':lemma') return;
      if (predicate === ':synset') return;
      
      const predLabel = predicate.replace(/^:/, '').replace(/^.*[#/]/, '');
      
      // Determine object type
      let objType = 'instance';
      let objLabel = object;
      
      if (object.startsWith('"')) {
        objType = 'literal';
        objLabel = object.replace(/^"|"$/g, '');
      } else if (object.startsWith('<') && object.endsWith('>')) {
        // Full IRI reference — show just the local name
        objLabel = object.replace(/^<|>$/g, '');
        if (objLabel.includes('/')) objLabel = objLabel.substring(objLabel.lastIndexOf('/') + 1);
        if (objLabel.includes('#')) objLabel = objLabel.substring(objLabel.lastIndexOf('#') + 1);
        objLabel = decodeURIComponent(objLabel);
      }
      
      if (!nodeMap.has(object)) {
        nodeMap.set(object, { id: object, label: objLabel, type: objType });
        nodes.push(nodeMap.get(object));
      }
      
      links.push({
        source: subject,
        target: object,
        label: predLabel,
        inferred: false
      });
    });
  });
  
  return { nodes, links };
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
  const hasMeaningfulContent = (getElement('ttlInput')?.value || '').replace(/^@prefix[^\n]*\n/gm, '').trim().length > 0;
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