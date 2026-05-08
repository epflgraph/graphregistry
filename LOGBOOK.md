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
- 👉 Keep modifying input formats in API endpoints
- ✅ Node endpoints are mostly fixed
- 👉 Edge endpoints need work, fix output format of 'get edge'. also, 'get many edges' is not returning results