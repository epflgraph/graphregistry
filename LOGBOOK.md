# Logbook

# 2026

### March
- Testing simple registry inserts with new pydantic models
- Working on CLI command to import data
- Using sample files `sample_course_{node,edge}.json`

### April
- I can now import data with
    ```bash
    graphregistry data insert --edge_list=@scripts/init/sample_sets/epfl_graph_sample_set_EDGEs.json --actions=commit
    ```
    ```bash
    graphregistry data insert --node_list=@scripts/init/sample_sets/epfl_graph_sample_set_NODEs.json --actions=commit
    ```
    but `NULL` fields in Nodes are being set as `None`. [FIXED]
- Edge custom fields are not being imported. [FIXED]
- Test full run with new structure [DONE]
- Think about where to store custom print functions (eg, print_node_saved(), print_edge_saved()) [DONE 1st version]
- Implement data file structure analyser/inspector
- Add `list` function to workflows/operations