# Logbook

# 2026

### March
- ✅ Testing simple registry inserts with new pydantic models
- ✅ Working on CLI command to import data
- ✅ Using sample files `sample_course_{node,edge}.json`

### April
- ✅ I can now import data with
    ```bash
    graphregistry data insert --edge_list=@scripts/init/sample_sets/epfl_graph_sample_set_EDGEs.json --actions=commit
    ```
    ```bash
    graphregistry data insert --node_list=@scripts/init/sample_sets/epfl_graph_sample_set_NODEs.json --actions=commit
    ```
    but `NULL` fields in Nodes are being set as `None`. [FIXED]
- ✅ Edge custom fields are not being imported. [FIXED]
- ✅ Test full run with new structure [DONE]
- 👉 Think about where to store custom print functions (eg, print_node_saved(), print_edge_saved()) [DONE 1st version]
- 👉 Implement data file structure analyser/inspector
- ✅ Add `list` function to workflows/operations [DONE]
- ✅ Fix CLI to use mappers [DONE]
- 👉 There's a problem with graphdb, in that the exporting function must be different depending on whether the mysql and mysqldump binaries are local or docker-based
- 👉 Also check if sql.gz files can be imported with the same function, or if they need to be unzipped first
- ✅ Now nodes can be linked to concepts after concept detection, but there's still no support for saving the concepts on mysql repo class. Need to implement together with delete function [DONE]

### May
- ✅ Implement mysql mapping function for concepts [DONE]
- ✅ Edge custom fields are not being imported. [FIXED]
- ✅ Keep modifying input formats in API endpoints
- ✅ Node endpoints are mostly fixed
- ✅ Edge endpoints need work, fix output format of 'get edge'. also, 'get many edges' is not returning results
- 👉 need to fix global/local paths issue in graphdb repo
- ✅ ./examples/entrypoints/node_list/api.sh is still returning DB format json, not API format
- ✅ this is not working: ./examples/entrypoints/node_delete/api.sh
- ✅ Why is `SELECT * FROM _1_DEV_graphsearch_test.Index_D_Course_L_Course_T_SEM;` empy?! See comment `GRW24tg`
- 👉 In `run2.sh`, add alternative way of importing data using API endpoints
- 👉 working on `t_video.py` script
- ✅ in graphdb, the copy command is not working [solution: missing temporary export folder parameter in global config]

### June
- ✅ Create a lecture sample set for dev
- 👉 execute all operations with the operations object, not with the repo class directly
- 👉 centralize initalization of operations object in a single function
- 👉 implement handling of "file not found" in graphai endpoints
- 👉 make db exports always overwrite the export folder, or at least ask for confirmation

### July
- 👉 `avg_scores` and `log_degrees` formulas take a long time even with no active flags
- 👉 make distinction between typeflags and airflow in reset cache operation
- 👨🏻‍💻 implement soft delete throughout cache updates

### August
- 👉 create nodes patching table on registry, to apply on every update cycle. Start with C***a L***t request
- 👉 investigate startups not showing in ES search
- 👉 check Execises out-of-date data
- 👨🏻‍💻 fix repeated `row_rank` values bug after re-ranking query
- 👨🏻‍💻 check 404 errors in API: fix with
- 👨🏻‍💻 clean out object-to-link tables (eg, graph_cache.Edges_N_Object_N_Concept_T_ScoringMatrix) by object_id before updating
- 👉 change all tables charset/collates to `CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`
- 👉 add missing concepts in ontology that come from exoset
- 👉 propagate `record_deleted` flags from `graph_registry` downstream to `graph_cache`. NOTE: current queries are way too slow; need to execute in chunks
- weird video length bug in https://graphsearch.epfl.ch/en/lecture/0_r6l7tub3
