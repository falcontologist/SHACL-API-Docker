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
  ENTITY_CATEGORIES: ["Person_Entity", "Organization_Entity", "Geopolitical_Entity", "Product_Entity", "Unit_Entity", "Occupation_Entity", "Creative_Work_Entity", "Quantity_Dimension_Entity", "Location_Entity", "Food_Entity", "Language_Entity", "Organism_Entity", "Equity_Entity", "Index_Entity", "Corporate_Bond_Entity", "Government_Bond_Entity"],
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
// DASH WIDGET REGISTRY
// Maps DASH editor/viewer local names to rendering strategies.
// Follows the DASH Forms spec scoring & widget selection logic.
// ============================================
const DASH = {
  // Editor types from the DASH namespace
  EDITORS: {
    AutoCompleteEditor: 'autocomplete',  // Entity search with FST suggest
    DetailsEditor: 'details',            // Nested sub-form (blank node)
    TextFieldEditor: 'textfield',        // Single-line literal input
    TextAreaEditor: 'textarea',          // Multi-line text
    TextAreaWithLangEditor: 'textarea',  // Multi-line with lang tag
    TextFieldWithLangEditor: 'textfield',// Single-line with lang tag
    DatePickerEditor: 'datepicker',      // xsd:date calendar
    DateTimePickerEditor: 'datetimepicker',
    BooleanSelectEditor: 'boolean',      // true/false dropdown
    EnumSelectEditor: 'enum',            // sh:in dropdown
    InstancesSelectEditor: 'instances',  // All instances of sh:class
    RichTextEditor: 'richtext',          // rdf:HTML
    URIEditor: 'uri',                    // Raw IRI input
    SubClassEditor: 'subclass',          // Class hierarchy picker
    BlankNodeEditor: 'blanknode',        // Read-only blank node
  },

  // Viewer types
  VIEWERS: {
    LabelViewer: 'label',
    LiteralViewer: 'literal',
    DetailsViewer: 'details',
    HTMLViewer: 'html',
    HyperlinkViewer: 'hyperlink',
    ImageViewer: 'image',
    URIViewer: 'uri',
    BlankNodeViewer: 'blanknode',
    LangStringViewer: 'langstring',
    ValueTableViewer: 'table',
  },

  /**
   * Determine the widget type for a field based on DASH metadata.
   * Follows the DASH scoring algorithm: explicit dash:editor wins,
   * then infer from sh:nodeKind + sh:or constraints.
   */
  resolveWidgetType(field) {
    // 1. Explicit dash:editor takes priority
    if (field.editor) {
      const mapped = this.EDITORS[field.editor];
      if (mapped) return mapped;
    }

    // 2. sh:in present → EnumSelectEditor (score 10)
    if (field.in && field.in.length > 0) return 'enum';

    // 3. Infer from nodeKind + constraints
    const nodeKind = field.nodeKind;
    const hasDatatypes = field.allowedDatatypes && field.allowedDatatypes.length > 0;
    const hasClasses = field.allowedClasses && field.allowedClasses.length > 0;

    // Literal fields
    if (nodeKind === 'Literal') {
      if (hasDatatypes) {
        const dts = field.allowedDatatypes;
        if (dts.includes('date') || dts.includes('gYearMonth') || dts.includes('gYear')) {
          return 'datepicker';
        }
        if (dts.includes('dateTime')) return 'datetimepicker';
        if (dts.includes('boolean')) return 'boolean';
        if (dts.includes('decimal') || dts.includes('integer') || dts.includes('float') || dts.includes('double')) {
          return 'number';
        }
      }
      // Default literal → textfield
      if (field.singleLine === false) return 'textarea';
      return 'textfield';
    }

    // BlankNode → DetailsEditor for nested forms
    if (nodeKind === 'BlankNode') return 'details';

    // BlankNodeOrIRI with classes → AutoCompleteEditor
    if (nodeKind === 'BlankNodeOrIRI' || nodeKind === 'IRI') {
      if (hasClasses) return 'autocomplete';
      return 'uri';
    }

    // Fallback: if there are allowed classes, use autocomplete
    if (hasClasses) return 'autocomplete';

    // Final fallback: textfield
    return 'textfield';
  },

  /**
   * Extract entity category options from sh:or class constraints.
   * Filters to only include categories that exist in the FST index.
   */
  extractEntityCategories(field) {
    if (!field.allowedClasses) return CONFIG.ENTITY_CATEGORIES;

    // Filter to entity categories that are in the allowed list
    const allowed = new Set(field.allowedClasses);
    const categories = CONFIG.ENTITY_CATEGORIES.filter(c => allowed.has(c));

    // If none match the standard entity categories, return all
    return categories.length > 0 ? categories : CONFIG.ENTITY_CATEGORIES;
  }
};

// ============================================
// STATE MANAGEMENT
// ============================================
const State = {
  situationMap: new Map(),
  nestedShapes: {},         // Non-situation shapes for DetailsEditor
  currentSenses: [],
  globalCount: 0,
  selectedSituation: null,
  currentVerb: "",
  
  // Entity suggest state (per-row, keyed by row index)
  entitySuggestControllers: new Map(),
  
  // Graph state
  simulation: null,
  svg: null,
  g: null,
  zoom: null,
  
  reset() {
    this.currentSenses = [];
    this.selectedSituation = null;
    this.currentVerb = "";
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
  const lookupBtn = getElement('lookupBtn');
  const verbInput = getElement('verbInput');
  if (lookupBtn) lookupBtn.addEventListener('click', handleLookup);
  if (verbInput) {
    verbInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') handleLookup();
    });
  }

  const senseSelect = getElement('senseSelect');
  const sitSelect = getElement('situationSelect');
  if (senseSelect) senseSelect.addEventListener('change', handleSenseSelect);
  if (sitSelect) sitSelect.addEventListener('change', handleSituationSelect);

  const addBtn = getElement('addEntryBtn');
  const inferBtn = getElement('inferBtn');
  const validateBtn = getElement('validateBtn');
  if (addBtn) addBtn.addEventListener('click', addEntry);
  if (inferBtn) inferBtn.addEventListener('click', runInference);
  if (validateBtn) validateBtn.addEventListener('click', validateGraph);

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
    const statsResp = await fetch(`${CONFIG.API_BASE_URL}/stats`);
    if (!statsResp.ok) throw new Error(`Stats endpoint returned ${statsResp.status}`);
    
    const stats = await statsResp.json();
    
    updateStatus(apiText, "Online", "#10b981");
    if (apiDot) apiDot.classList.add('online');
    
    ['Shapes', 'Roles', 'Rules', 'Lemmas', 'Senses'].forEach(key => {
      const el = getElement(`count${key}`);
      if (el) el.textContent = stats[key.toLowerCase()] || 0;
    });

    // Fetch DASH-compliant form definitions
    const formResp = await fetch(`${CONFIG.API_BASE_URL}/forms`);
    if (formResp.ok) {
      const formData = await formResp.json();

      // Store full shape definitions (with DASH metadata)
      Object.entries(formData.forms).forEach(([shapeId, shapeDef]) => {
        State.situationMap.set(shapeId, shapeDef);
      });

      // Store nested shapes (e.g. Cost_shape) for DetailsEditor
      if (formData.nestedShapes) {
        State.nestedShapes = formData.nestedShapes;
      }

      console.log(`✓ Loaded ${State.situationMap.size} DASH-compliant shape definitions`);
      if (Object.keys(State.nestedShapes).length > 0) {
        console.log(`  └── ${Object.keys(State.nestedShapes).length} nested shapes (DetailsEditor targets)`);
      }
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
 * @param {string} typeParam - "all", single category, or comma-separated categories
 * @param {string} query - search text
 * @param {AbortSignal} signal - abort controller signal
 * @param {string} [roleClass] - optional role player class for score boosting
 */
async function fetchEntitySuggestions(typeParam, query, signal, roleClass) {
  let url = `${CONFIG.API_BASE_URL}/entity-suggest?type=${encodeURIComponent(typeParam)}&q=${encodeURIComponent(query)}&limit=${CONFIG.SUGGEST_MAX_RESULTS}`;
  if (roleClass) {
    url += `&role=${encodeURIComponent(roleClass)}`;
  }
  const resp = await fetch(url, { signal });
  if (!resp.ok) throw new Error(`Suggest returned ${resp.status}`);
  return resp.json();
}

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
  const shapeDef = State.situationMap.get(selectedShapeID);
  
  const hint = getElement('domainHint');
  if (hint && shapeDef) {
    // Show shape description + example from DASH metadata
    let hintText = `Shape: ${selectedShapeID}`;
    if (shapeDef.description) {
      hintText = shapeDef.description;
    }
    hint.textContent = hintText;
    hint.title = shapeDef.example || '';
  } else if (hint) {
    hint.textContent = "No SHACL definition found";
  }
    
  renderForm(shapeDef);
}

// ============================================
// DASH-DRIVEN FORM RENDERER
// Reads SHACL + DASH metadata and renders the
// appropriate widget for each property shape.
// ============================================
function renderForm(shapeDef) {
  const form = getElement('dynamicForm');
  if (!form) return;
  
  form.innerHTML = '';
  
  if (!shapeDef || !shapeDef.fields) {
    hideElement('stepForm');
    return;
  }

  // Show example sentence if available (skos:example)
  if (shapeDef.example) {
    const exampleDiv = document.createElement('div');
    exampleDiv.className = 'shape-example';
    exampleDiv.innerHTML = `<span class="example-label">Example:</span> ${escapeHtml(shapeDef.example)}`;
    form.appendChild(exampleDiv);
  }

  // Fields are already sorted by sh:order from the backend
  shapeDef.fields.forEach((field, index) => {
    const widgetType = DASH.resolveWidgetType(field);
    const row = createDashRow(field, index, widgetType);
    form.appendChild(row);
  });
  
  showElement('stepForm');
}

/**
 * Creates a form row driven by DASH widget type.
 * Each field's widget is determined by its dash:editor, sh:nodeKind, and sh:or constraints.
 */
function createDashRow(field, rowIndex, widgetType) {
  const row = document.createElement('div');
  row.className = 'role-row';
  row.dataset.rowIndex = rowIndex;
  row.dataset.widgetType = widgetType;
  row.dataset.path = field.path || '';

  // Label with required indicator
  const labelEl = document.createElement('label');
  labelEl.className = 'role-label';
  labelEl.textContent = field.label + (field.required ? ' *' : '');
  if (field.description) {
    labelEl.title = field.description;
    labelEl.classList.add('has-tooltip');
  }
  row.appendChild(labelEl);

  // Description hint (sh:description)
  if (field.description) {
    const descEl = document.createElement('div');
    descEl.className = 'field-description';
    descEl.textContent = field.description;
    row.appendChild(descEl);
  }

  // Render the appropriate widget
  const widgetContainer = document.createElement('div');
  widgetContainer.className = 'compact-row';

  switch (widgetType) {
    case 'autocomplete':
      renderAutoCompleteWidget(widgetContainer, field, rowIndex);
      break;
    case 'details':
      renderDetailsWidget(widgetContainer, field, rowIndex);
      break;
    case 'datepicker':
      renderDateWidget(widgetContainer, field, rowIndex);
      break;
    case 'datetimepicker':
      renderDateTimeWidget(widgetContainer, field, rowIndex);
      break;
    case 'number':
      renderNumberWidget(widgetContainer, field, rowIndex);
      break;
    case 'enum':
      renderEnumWidget(widgetContainer, field, rowIndex);
      break;
    case 'boolean':
      renderBooleanWidget(widgetContainer, field, rowIndex);
      break;
    case 'textarea':
      renderTextAreaWidget(widgetContainer, field, rowIndex);
      break;
    case 'uri':
      renderURIWidget(widgetContainer, field, rowIndex);
      break;
    case 'textfield':
    default:
      renderTextFieldWidget(widgetContainer, field, rowIndex);
      break;
  }

  row.appendChild(widgetContainer);
  return row;
}

// ============================================
// DASH WIDGET RENDERERS
// Each corresponds to a dash:Editor from the spec.
// ============================================

/**
 * dash:AutoCompleteEditor — entity search with FST suggest.
 * 
 * Layout:
 *   [Type: Entity ▾] [🔍 All Entities ▾] [Search input...] [Sense ▾]
 *
 * The category filter button defaults to "All Entities" (searches across
 * all 17 FST indices). Clicking it opens a checkbox popover where the user
 * can select specific categories to narrow the search.
 *
 * The field's sh:class (role player class) is passed to the API as a
 * score-boosting hint — entities that have filled this role before will
 * rank higher.
 */
function renderAutoCompleteWidget(container, field, rowIndex) {
  // Extract the role player class from the field's sh:class (e.g. "Acquirer_Role_Player")
  const roleClass = field.defaultClass || field['class'] || '';

  container.innerHTML = `
    <select class="type-select">
      <option value="Entity">Entity</option>
      <option value="Instance">Instance</option>
      <option value="Literal">Literal</option>
      <option value="IRI">IRI</option>
      <option value="BNode">_:</option>
    </select>
    <div class="entity-suggest-wrapper">
      <div class="category-filter-wrapper">
        <button type="button" class="category-filter-btn" title="Filter entity categories">
          <span class="category-filter-label">All Entities</span>
          <span class="category-filter-chevron">▾</span>
        </button>
        <div class="category-filter-popover hidden">
          <div class="category-filter-header">
            <span>Filter categories</span>
            <button type="button" class="category-filter-toggle-all">Select all</button>
          </div>
          <div class="category-filter-list">
            ${CONFIG.ENTITY_CATEGORIES.map(c => `
              <label class="category-filter-item">
                <input type="checkbox" value="${c}" checked>
                <span>${formatCategoryLabel(c)}</span>
              </label>
            `).join('')}
          </div>
        </div>
      </div>
      <div class="suggest-input-container">
        <input class="entity-search-input" type="text" 
               placeholder="Search entities..." 
               autocomplete="off"
               data-role="${field.label}" 
               data-path="${field.path}"
               data-role-class="${roleClass}">
        <div class="suggest-spinner hidden"></div>
        <div class="suggest-dropdown hidden"></div>
      </div>
      <select class="entity-sense-select hidden">
        <option value="">-- Select Sense --</option>
      </select>
    </div>
    <input class="role-input hidden" type="text" 
           data-role="${field.label}" 
           data-path="${field.path}" 
           placeholder="Value..." 
           ${field.required ? 'required' : ''}>
    <select class="instance-select hidden" style="flex: 1;">
      <option value="">-- Select Instance --</option>
    </select>
  `;

  // Wire type switching
  const typeSelect = container.querySelector('.type-select');
  const entityWrapper = container.querySelector('.entity-suggest-wrapper');
  const textInput = container.querySelector('.role-input');
  const instanceSelect = container.querySelector('.instance-select');

  typeSelect.addEventListener('change', () => {
    const val = typeSelect.value;
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

  entityWrapper.classList.remove('hidden');
  textInput.classList.add('hidden');
  instanceSelect.classList.add('hidden');

  // Wire category filter popover
  wireCategoryFilter(container);

  // Wire entity autocomplete
  wireAutoComplete(container, rowIndex, field);
}

/**
 * Wire the category filter button + checkbox popover.
 */
function wireCategoryFilter(container) {
  const btn = container.querySelector('.category-filter-btn');
  const popover = container.querySelector('.category-filter-popover');
  const toggleAllBtn = container.querySelector('.category-filter-toggle-all');
  const checkboxes = container.querySelectorAll('.category-filter-item input[type="checkbox"]');
  const labelSpan = container.querySelector('.category-filter-label');

  if (!btn || !popover) return;

  // Toggle popover
  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    // Close other open popovers first
    document.querySelectorAll('.category-filter-popover').forEach(p => {
      if (p !== popover) p.classList.add('hidden');
    });
    popover.classList.toggle('hidden');
  });

  // Close popover when clicking outside
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.category-filter-wrapper')) {
      popover.classList.add('hidden');
    }
  });

  // Prevent popover clicks from closing it
  popover.addEventListener('click', (e) => {
    e.stopPropagation();
  });

  // Toggle all / none
  toggleAllBtn.addEventListener('click', () => {
    const allChecked = Array.from(checkboxes).every(cb => cb.checked);
    checkboxes.forEach(cb => { cb.checked = !allChecked; });
    toggleAllBtn.textContent = allChecked ? 'Select all' : 'Clear all';
    updateCategoryLabel(checkboxes, labelSpan, toggleAllBtn);
  });

  // Update label on individual checkbox change
  checkboxes.forEach(cb => {
    cb.addEventListener('change', () => {
      updateCategoryLabel(checkboxes, labelSpan, toggleAllBtn);
    });
  });
}

/**
 * Update the filter button label based on checkbox state.
 */
function updateCategoryLabel(checkboxes, labelSpan, toggleAllBtn) {
  const checked = Array.from(checkboxes).filter(cb => cb.checked);
  const total = checkboxes.length;

  if (checked.length === 0) {
    labelSpan.textContent = 'None selected';
    labelSpan.classList.add('category-filter-warn');
  } else if (checked.length === total) {
    labelSpan.textContent = 'All Entities';
    labelSpan.classList.remove('category-filter-warn');
    toggleAllBtn.textContent = 'Clear all';
  } else if (checked.length <= 2) {
    labelSpan.textContent = checked.map(cb => formatCategoryLabel(cb.value)).join(', ');
    labelSpan.classList.remove('category-filter-warn');
    toggleAllBtn.textContent = 'Select all';
  } else {
    labelSpan.textContent = `${checked.length} categories`;
    labelSpan.classList.remove('category-filter-warn');
    toggleAllBtn.textContent = 'Select all';
  }
}

/**
 * Get the type parameter string from the category filter checkboxes.
 * Returns "all" if all are checked, or comma-separated category names.
 */
function getCategoryTypeParam(container) {
  const checkboxes = container.querySelectorAll('.category-filter-item input[type="checkbox"]');
  const checked = Array.from(checkboxes).filter(cb => cb.checked).map(cb => cb.value);
  
  if (checked.length === 0) return 'all'; // Fallback: search everything
  if (checked.length === checkboxes.length) return 'all';
  return checked.join(',');
}

/**
 * dash:DetailsEditor — nested sub-form for blank nodes.
 * Renders fields from the sh:node shape inline.
 */
function renderDetailsWidget(container, field, rowIndex) {
  container.className = 'details-widget-container';

  // Check for nested shape definition
  const nestedFields = field.nestedFields;
  const nestedShapeId = field.nodeShape;
  const shapeDef = nestedFields ? { fields: nestedFields }
    : (nestedShapeId && State.nestedShapes[nestedShapeId])
      ? State.nestedShapes[nestedShapeId] : null;

  if (!shapeDef || !shapeDef.fields || shapeDef.fields.length === 0) {
    // Fallback: plain text input
    container.innerHTML = `
      <input class="role-input" type="text" 
             data-role="${field.label}" 
             data-path="${field.path}" 
             placeholder="${field.label} value..." 
             ${field.required ? 'required' : ''}>
    `;
    return;
  }

  const nestedForm = document.createElement('div');
  nestedForm.className = 'nested-form';
  nestedForm.dataset.nodeShape = nestedShapeId || '';
  nestedForm.dataset.parentPath = field.path || '';

  const header = document.createElement('div');
  header.className = 'nested-form-header';
  header.textContent = shapeDef.label || field.label || nestedShapeId;
  if (shapeDef.description) header.title = shapeDef.description;
  nestedForm.appendChild(header);

  // Render each nested field with its own DASH widget
  shapeDef.fields.forEach((nestedField, nestedIndex) => {
    const nestedWidgetType = DASH.resolveWidgetType(nestedField);
    const nestedRow = createDashRow(nestedField, rowIndex * 100 + nestedIndex, nestedWidgetType);
    nestedRow.classList.add('nested-role-row');
    nestedForm.appendChild(nestedRow);
  });

  container.appendChild(nestedForm);
}

/**
 * dash:LiteralViewer with date datatypes — date input.
 * Supports xsd:date, xsd:gYearMonth, xsd:gYear.
 */
function renderDateWidget(container, field, rowIndex) {
  const dts = field.allowedDatatypes || [];
  
  // Determine most specific date type
  let inputType = 'date';
  let placeholder = 'YYYY-MM-DD';
  
  if (dts.includes('gYear') && !dts.includes('date') && !dts.includes('gYearMonth')) {
    inputType = 'number';
    placeholder = 'YYYY';
  } else if (dts.includes('gYearMonth') && !dts.includes('date')) {
    inputType = 'month';
    placeholder = 'YYYY-MM';
  }

  // Show a type switcher for mixed date constraints
  if (dts.length > 1) {
    container.innerHTML = `
      <select class="date-type-select">
        ${dts.includes('date') ? '<option value="date">Date (YYYY-MM-DD)</option>' : ''}
        ${dts.includes('gYearMonth') ? '<option value="month">Month (YYYY-MM)</option>' : ''}
        ${dts.includes('gYear') ? '<option value="year">Year (YYYY)</option>' : ''}
      </select>
      <input class="role-input date-input" type="${inputType}" 
             data-role="${field.label}" 
             data-path="${field.path}" 
             placeholder="${placeholder}">
    `;

    const typeSelect = container.querySelector('.date-type-select');
    const input = container.querySelector('.date-input');
    
    typeSelect.addEventListener('change', () => {
      const val = typeSelect.value;
      if (val === 'date') { input.type = 'date'; input.placeholder = 'YYYY-MM-DD'; }
      else if (val === 'month') { input.type = 'month'; input.placeholder = 'YYYY-MM'; }
      else { input.type = 'number'; input.placeholder = 'YYYY'; input.min = '0'; input.max = '9999'; }
    });
  } else {
    container.innerHTML = `
      <input class="role-input date-input" type="${inputType}" 
             data-role="${field.label}" 
             data-path="${field.path}" 
             placeholder="${placeholder}"
             ${inputType === 'number' ? 'min="0" max="9999"' : ''}>
    `;
  }
}

/**
 * dash:DateTimePickerEditor — datetime input.
 */
function renderDateTimeWidget(container, field, rowIndex) {
  container.innerHTML = `
    <input class="role-input" type="datetime-local" 
           data-role="${field.label}" 
           data-path="${field.path}">
  `;
}

/**
 * dash:TextFieldEditor for numeric types — number input.
 */
function renderNumberWidget(container, field, rowIndex) {
  const step = (field.allowedDatatypes || []).some(d => d === 'integer') ? '1' : 'any';
  container.innerHTML = `
    <input class="role-input" type="number" step="${step}"
           data-role="${field.label}" 
           data-path="${field.path}" 
           placeholder="${field.label}..."
           ${field.required ? 'required' : ''}>
  `;
}

/**
 * dash:EnumSelectEditor — dropdown from sh:in values.
 */
function renderEnumWidget(container, field, rowIndex) {
  container.innerHTML = `
    <select class="role-input enum-select"
            data-role="${field.label}" 
            data-path="${field.path}"
            ${field.required ? 'required' : ''}>
      <option value="">-- Select --</option>
      ${(field.in || []).map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('')}
    </select>
  `;
}

/**
 * dash:BooleanSelectEditor — true/false dropdown.
 */
function renderBooleanWidget(container, field, rowIndex) {
  container.innerHTML = `
    <select class="role-input"
            data-role="${field.label}" 
            data-path="${field.path}">
      <option value="">-- Select --</option>
      <option value="true">true</option>
      <option value="false">false</option>
    </select>
  `;
}

/**
 * dash:TextAreaEditor — multi-line text input.
 */
function renderTextAreaWidget(container, field, rowIndex) {
  container.innerHTML = `
    <textarea class="role-input textarea-widget" rows="3"
              data-role="${field.label}" 
              data-path="${field.path}" 
              placeholder="${field.label}..."
              ${field.required ? 'required' : ''}
              ${field.maxLength ? `maxlength="${field.maxLength}"` : ''}></textarea>
  `;
}

/**
 * dash:URIEditor — raw IRI input.
 */
function renderURIWidget(container, field, rowIndex) {
  container.innerHTML = `
    <input class="role-input uri-input" type="url" 
           data-role="${field.label}" 
           data-path="${field.path}" 
           placeholder="https://..."
           ${field.required ? 'required' : ''}>
  `;
}

/**
 * dash:TextFieldEditor — single-line text input (default fallback).
 */
function renderTextFieldWidget(container, field, rowIndex) {
  const attrs = [];
  if (field.pattern) attrs.push(`pattern="${escapeHtml(field.pattern)}"`);
  if (field.minLength) attrs.push(`minlength="${field.minLength}"`);
  if (field.maxLength) attrs.push(`maxlength="${field.maxLength}"`);
  if (field.required) attrs.push('required');

  container.innerHTML = `
    <input class="role-input" type="text" 
           data-role="${field.label}" 
           data-path="${field.path}" 
           placeholder="${field.label}..."
           ${attrs.join(' ')}>
  `;
}

// ============================================
// AUTOCOMPLETE WIRING (for AutoCompleteEditor)
// ============================================
function wireAutoComplete(container, rowIndex, field) {
  const searchInput = container.querySelector('.entity-search-input');
  const dropdown = container.querySelector('.suggest-dropdown');
  const spinner = container.querySelector('.suggest-spinner');
  const senseSelect = container.querySelector('.entity-sense-select');

  if (!searchInput || !dropdown) return;

  // Extract role class for score boosting
  const roleClass = field.defaultClass || field['class'] || '';

  const debouncedSuggest = debounce(async () => {
    const query = searchInput.value.trim();
    
    if (query.length < CONFIG.SUGGEST_MIN_CHARS) {
      dropdown.classList.add('hidden');
      return;
    }

    // Get the category type param from the checkbox filter
    const typeParam = getCategoryTypeParam(container);

    const prevController = State.entitySuggestControllers.get(rowIndex);
    if (prevController) prevController.abort();
    
    const controller = new AbortController();
    State.entitySuggestControllers.set(rowIndex, controller);

    if (spinner) spinner.classList.remove('hidden');

    try {
      const data = await fetchEntitySuggestions(typeParam, query, controller.signal, roleClass);
      renderSuggestDropdown(dropdown, data.results, searchInput, senseSelect, container.closest('.role-row'));
      if (spinner) spinner.classList.add('hidden');
    } catch (err) {
      if (err.name !== 'AbortError') {
        console.error('Suggest error:', err);
        if (spinner) spinner.classList.add('hidden');
      }
    }
  }, CONFIG.SUGGEST_DEBOUNCE_MS);

  searchInput.addEventListener('input', debouncedSuggest);

  // Re-query when category filter changes
  const checkboxes = container.querySelectorAll('.category-filter-item input[type="checkbox"]');
  checkboxes.forEach(cb => {
    cb.addEventListener('change', () => {
      if (searchInput.value.trim().length >= CONFIG.SUGGEST_MIN_CHARS) {
        debouncedSuggest();
      }
    });
  });

  // Keyboard navigation
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

  searchInput.addEventListener('blur', () => {
    setTimeout(() => dropdown.classList.add('hidden'), 200);
  });
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

  if (!senseSelect) return;

  try {
    const data = await fetchEntitySenses(item.iri);
    
    if (data.senses && data.senses.length > 0) {
      senseSelect.innerHTML = '<option value="">-- Select Sense --</option>';
      data.senses.forEach(sense => {
        const opt = document.createElement('option');
        opt.value = sense.senseIRI;
        const displayLabel = sense.label || sense.senseId;
        const gloss = sense.gloss && sense.gloss.trim() ? sense.gloss.trim() : '';
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

      if (data.senses.length === 1) {
        senseSelect.value = data.senses[0].senseIRI;
      }
    } else {
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
    case 'Creative_Work_Entity': return 'Creative Work';
    case 'Quantity_Dimension_Entity': return 'Quantity Dimension';
    case 'Location_Entity': return 'Location';
    case 'Food_Entity': return 'Food';
    case 'Language_Entity': return 'Language';
    case 'Organism_Entity': return 'Organism';
    case 'Equity_Entity': return 'Equity';
    case 'Index_Entity': return 'Index';
    case 'Corporate_Bond_Entity': return 'Corporate Bond';
    case 'Government_Bond_Entity': return 'Government Bond';
    default: return category.replace(/_Entity$/, '');
  }
}

function populateInstanceSelect(selectElement) {
  const ttl = getElement('ttlInput')?.value || '';
  const instances = [];
  
  const pattern = /temp:s(\d+)\s+a\s+:(\w+)\s*;[\s\S]*?rdfs:label\s+"([^"]+)"/g;
  let match;
  
  while ((match = pattern.exec(ttl)) !== null) {
    instances.push({
      id: `temp:s${match[1]}`,
      className: match[2],
      label: match[3]
    });
  }
  
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
// ADD ENTRY (DASH-aware)
// ============================================
async function addEntry() {
  State.globalCount++;
  const sitId = `temp:s${State.globalCount}`;
  const ttlArea = getElement('ttlInput');
  
  if (!ttlArea) return;
  
  const selectedIndex = getElement('senseSelect')?.value;
  const sense = State.currentSenses[selectedIndex];
  
  const className = State.selectedSituation.replace('_shape', '');
  let mainBlock = `${sitId} a :${className} ;\n    rdfs:label "${State.currentVerb}" ;\n    :lemma "${State.currentVerb}"`;
  
  if (sense && sense.gloss) {
    mainBlock += ` ;\n    :synset "${sense.gloss}"`;
  }
  
  let entityBlock = "";

  // Process all role rows (including nested forms)
  document.querySelectorAll('#dynamicForm > .role-row').forEach(row => {
    const widgetType = row.dataset.widgetType;
    const result = collectRowValue(row, widgetType, ttlArea.value);
    if (result) {
      mainBlock += result.main;
      entityBlock += result.entity;
    }
  });

  const newData = `${mainBlock} .\n${entityBlock}\n`;
  ttlArea.value += newData;
  
  updateGraph();
  showSuccess('addEntryBtn', "✓ Added");
}

/**
 * Collect the value from a form row based on its DASH widget type.
 */
function collectRowValue(row, widgetType, existingTTL) {
  const path = row.dataset.path;
  if (!path) return null;
  
  const pathLocal = path.split(/[#/]/).pop();
  const predicate = `:${pathLocal}`;

  if (widgetType === 'autocomplete') {
    return collectAutoCompleteValue(row, predicate, existingTTL);
  }
  
  if (widgetType === 'details') {
    return collectDetailsValue(row, predicate, existingTTL);
  }

  // All other widgets: read from .role-input
  const input = row.querySelector('.role-input');
  if (!input) return null;
  
  const value = (input.value || '').trim();
  if (!value) return null;

  // Date types → xsd:date / xsd:gYearMonth / xsd:gYear
  if (widgetType === 'datepicker') {
    const dtSelect = row.querySelector('.date-type-select');
    const dtType = dtSelect ? dtSelect.value : 'date';
    const xsdType = dtType === 'year' ? 'xsd:gYear' : dtType === 'month' ? 'xsd:gYearMonth' : 'xsd:date';
    return { main: ` ;\n    ${predicate} "${value}"^^${xsdType}`, entity: '' };
  }

  if (widgetType === 'number') {
    return { main: ` ;\n    ${predicate} "${value}"^^xsd:decimal`, entity: '' };
  }

  if (widgetType === 'boolean') {
    return { main: ` ;\n    ${predicate} "${value}"^^xsd:boolean`, entity: '' };
  }

  if (widgetType === 'uri') {
    const iri = value.startsWith('<') ? value : value.includes(':') ? value : `<${value}>`;
    return { main: ` ;\n    ${predicate} ${iri}`, entity: '' };
  }

  // Default: literal string
  return { main: ` ;\n    ${predicate} "${value}"`, entity: '' };
}

/**
 * Collect value from an AutoCompleteEditor row.
 */
function collectAutoCompleteValue(row, predicate, existingTTL) {
  const typeSelect = row.querySelector('.type-select');
  const type = typeSelect ? typeSelect.value : 'Entity';

  if (type === 'Entity') {
    const searchInput = row.querySelector('.entity-search-input');
    const resolvedIri = searchInput?.dataset.resolvedIri;
    const value = searchInput?.value.trim() || '';
    if (!value) return null;

    if (resolvedIri) {
      return { main: ` ;\n    ${predicate} <${resolvedIri}>`, entity: '' };
    } else {
      const slug = toSlug(value);
      const tempIRI = `temp:${slug}`;
      let entity = '';
      if (!existingTTL.includes(`${tempIRI} a :Entity`)) {
        entity = `${tempIRI} a :Entity ;\n    rdfs:label "${value}" .\n`;
      }
      return { main: ` ;\n    ${predicate} ${tempIRI}`, entity };
    }
  }

  if (type === 'Instance') {
    const instanceSelect = row.querySelector('.instance-select');
    const value = instanceSelect?.value;
    if (!value) return null;
    return { main: ` ;\n    ${predicate} ${value}`, entity: '' };
  }

  // Literal, IRI, BNode
  const textInput = row.querySelector('.role-input');
  const value = textInput?.value.trim();
  if (!value) return null;

  return buildTriple(type, value, predicate, existingTTL);
}

/**
 * Collect value from a DetailsEditor (nested form) row.
 * Creates a blank node with the nested properties.
 */
function collectDetailsValue(row, predicate, existingTTL) {
  const nestedForm = row.querySelector('.nested-form');
  if (!nestedForm) {
    // Fallback: simple input
    const input = row.querySelector('.role-input');
    const value = input?.value.trim();
    if (!value) return null;
    return { main: ` ;\n    ${predicate} "${value}"`, entity: '' };
  }

  // Collect nested field values
  const nestedRows = nestedForm.querySelectorAll('.nested-role-row');
  let bnodeBody = '';
  let nestedEntities = '';
  const nodeShape = nestedForm.dataset.nodeShape;

  nestedRows.forEach(nestedRow => {
    const nestedWidgetType = nestedRow.dataset.widgetType;
    const result = collectRowValue(nestedRow, nestedWidgetType, existingTTL);
    if (result) {
      bnodeBody += result.main;
      nestedEntities += result.entity;
    }
  });

  if (!bnodeBody) return null;

  // Emit as a blank node
  const bnodeTriples = bnodeBody.replace(/^ ;\n    /, '');
  return {
    main: ` ;\n    ${predicate} [\n        ${bnodeTriples.replace(/\n/g, '\n        ')}\n    ]`,
    entity: nestedEntities
  };
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

  d3.select("#graph-container").selectAll("*").remove();

  State.svg = d3.select("#graph-container")
    .append("svg")
    .attr("width", "100%")
    .attr("height", "100%");

  State.zoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on("zoom", (event) => {
      if (State.g) State.g.attr("transform", event.transform);
    });
  
  State.svg.call(State.zoom);

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

  State.g = State.svg.append("g");

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
  
  State.g.selectAll(".link").remove();
  State.g.selectAll(".node").remove();
  State.g.selectAll(".link-label-group").remove();
  
  if (nodes.length === 0) return;

  const link = State.g.selectAll(".link")
    .data(links)
    .join("line")
    .attr("class", d => `link${d.inferred ? ' inferred' : ''}`)
    .attr("marker-end", "url(#arrowhead)");

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

  const saveBtn = getElement('saveBtn');
  if (saveBtn) {
    const hasMeaningfulContent = (getElement('ttlInput')?.value || '').replace(/^@prefix[^\n]*\n/gm, '').trim().length > 0;
    saveBtn.disabled = !hasMeaningfulContent;
  }

  State.simulation.force("link").links(links);
  State.simulation.alpha(1).restart();
  
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
  
  const blocks = clean.split(/\.\s*\n/).filter(b => b.trim());
  
  blocks.forEach(block => {
    const lines = block.trim().split(/\s*;\s*\n\s*/);
    if (lines.length === 0) return;
    
    const firstLine = lines[0].trim();
    const firstParts = firstLine.match(/^(\S+)\s+(\S+)\s+(.*)/s);
    if (!firstParts) return;
    
    const subject = firstParts[1];
    let subjectLabel = subject;
    
    const labelMatch = block.match(/rdfs:label\s+"([^"]+)"/);
    if (labelMatch) subjectLabel = labelMatch[1];
    
    const typeMatch = block.match(/a\s+:(\w+)/);
    const nodeType = typeMatch ? 'instance' : 'class';
    
    if (!nodeMap.has(subject)) {
      nodeMap.set(subject, { id: subject, label: subjectLabel, type: nodeType });
      nodes.push(nodeMap.get(subject));
    }
    
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
      
      let objType = 'instance';
      let objLabel = object;
      
      if (object.startsWith('"')) {
        objType = 'literal';
        objLabel = object.replace(/^"|"$/g, '');
      } else if (object.startsWith('<') && object.endsWith('>')) {
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

function escapeHtml(str) {
  const div = document.createElement('div');
  div.appendChild(document.createTextNode(str));
  return div.innerHTML;
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
