-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS age;

-- Load AGE into the search path
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Create the routing graph
SELECT create_graph('alabama_routing');
