
#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_cache_to_process(args):
    """
    Handle:
      graphregistry cache to_process --count
      graphregistry cache to_process --reset
    """

    # Fetch context objects
    glbcfg = args.ctx.global_config
    db = args.ctx.db

    # Get list of tables where to_process flag is present
    list_of_tables = [
        table_name
        for table_name  in db.get_tables_in_schema(engine_name='test', schema_name=glbcfg.schema_graph_cache_test)
        if 'to_process' in db.get_column_names(    engine_name='test', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name)
    ]

    # Check arguments:
    # >> graphregistry cache to_process --count
    if args.count:

        # Print headers
        print("🖥️  ~ Graph Registry CLI. Count number of rows where 'to_process=1' in cache tables.")
        print(f"\n{'-'*78}\ntable_name{' '*(64 - len('table_name') + 2)}n_to_process\n{'-'*78}")

        # Execute command:
        # - for each table, count and print the number of rows where to_process=1
        for table_name in list_of_tables:
            n = db.count_rows_in_table(engine_name='test', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name, where_clause="to_process = 1")
            print(f"{table_name} {'.'*(64 - len(table_name))} {n}")

        # Print footers
        print('-'*78+'\n')
        print("🖥️  ~ Done.")

    # >> graphregistry cache to_process --reset
    elif args.reset:

        # Print headers
        print("🖥️  ~ Graph Registry CLI. Reset 'to_process' flags in cache tables.")

        # Execute command:
        # - for each table, set all cells to_process=0
        for table_name in list_of_tables:
            print(f"⚙️  Processing table: {table_name} ...")
            db.set_cells(engine_name='test', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name, set=[('to_process', 0)], where=[('to_process', 1)], verbose=False)

        # Print footers
        print("🖥️  ~ Done.")
