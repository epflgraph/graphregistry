
./examples/entrypoints/node_save_many/cli.sh
./examples/entrypoints/node_get_many/cli.sh
./examples/entrypoints/node_exists_many/cli.sh
./examples/entrypoints/node_delete_many/cli.sh

./examples/entrypoints/edge_save_many/cli.sh
./examples/entrypoints/edge_get_many/cli.sh
./examples/entrypoints/edge_exists_many/cli.sh
./examples/entrypoints/edge_delete_many/cli.sh




./examples/entrypoints/node_save_many/api.sh
./examples/entrypoints/node_get_many/api.sh
./examples/entrypoints/node_exists_many/api.sh
./examples/entrypoints/node_delete_many/api.sh

./examples/entrypoints/edge_save_many/api.sh
./examples/entrypoints/edge_get_many/api.sh
./examples/entrypoints/edge_exists_many/api.sh
./examples/entrypoints/edge_delete_many/api.sh


graphregistry data save --node_list examples/sample_sets/sample_epfl_node_list.json
graphregistry data save --edge_list examples/sample_sets/sample_epfl_edge_list.json

