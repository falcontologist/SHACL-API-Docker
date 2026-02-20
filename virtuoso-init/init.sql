-- =============================================================================
-- Virtuoso init script for shacl-demo
-- Named graphs:
--   http://shacl-demo.org/type   <- schematic layer (ontology partitions)
--   http://shacl-demo.org/token  <- token layer (situated event instances)
-- =============================================================================

-- 1. Create dedicated write account for the backend
DB.DBA.USER_CREATE ('sparql_writer', 'writer_secret');

-- 2. Load ontology partitions into the type graph via SPARQL 1.1 LOAD
SPARQL LOAD <https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/conceptual.ttl> INTO <http://shacl-demo.org/type>;
SPARQL LOAD <https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/structural.ttl> INTO <http://shacl-demo.org/type>;
SPARQL LOAD <https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/lexical.ttl> INTO <http://shacl-demo.org/type>;

-- 3. Set permissions
--    15 = read + write + list + sponge
--     1 = read only
DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('sparql_writer', 15);
DB.DBA.RDF_GRAPH_USER_PERMS_SET ('http://shacl-demo.org/type',  'sparql_writer', 15);
DB.DBA.RDF_GRAPH_USER_PERMS_SET ('http://shacl-demo.org/token', 'sparql_writer', 15);
DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 1);
DB.DBA.RDF_GRAPH_USER_PERMS_SET ('http://shacl-demo.org/type',  'nobody', 1);
DB.DBA.RDF_GRAPH_USER_PERMS_SET ('http://shacl-demo.org/token', 'nobody', 1);

COMMIT WORK;

-- 4. Verify - triple count printed to init logs
SPARQL SELECT COUNT(*) WHERE { GRAPH <http://shacl-demo.org/type> { ?s ?p ?o } };
