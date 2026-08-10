# graphregistry/entrypoints/cli/cmd_cache.py

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_cache_update(args):
    """
    Handle:
      graphregistry cache update [...]
    """

    # Fetch context objects
    registry = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Update cache from registry tables.")

    # Get input options
    formulas     = tuple(args.formulas.split(','))    if args.formulas     else ()
    actions      = tuple(args.actions.split(','))     if args.actions      else ()
    matrix       = args.matrix       if 'matrix'       in args else False
    formula_path = args.formula_path if 'formula_path' in args else None

    # -----------------#
    # Execute commands #
    # -----------------#
    if formula_path:
        registry.cachemanager.apply_formula_by_path(formula_path=formula_path, actions=actions)
    if 'fields' in formulas and 'commit' in actions:
        registry.cachemanager.apply_calculated_field_formulas(verbose='print' in actions, actions=actions)
    if 'views' in formulas:
        registry.cachemanager.materialize_views(actions=actions)
    if 'traversals' in formulas and 'commit' in actions:
        registry.cachemanager.apply_traversals(verbose='print' in actions, actions=actions)
    if 'scores' in formulas and 'commit' in actions:
        registry.cachemanager.apply_scoring_formulas(verbose='print' in actions, actions=actions)
    if matrix is True:
        registry.cachemanager.update_scores_matrix(score_thr=0.1, actions=actions)

    # Print footers
    print("🖥️  ~ Done.")

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
        for table_name  in db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test)
        if 'to_process' in db.get_column_names(    engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name)
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
            n = db.count_rows_in_table(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name, where_clause="to_process = 1")
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
            db.set_cells(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name, set=[('to_process', 0)], where=[('to_process', 1)], verbose=False)

        # Print footers
        print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_cache_debug(args):
    """
    Handle:
      graphregistry cache debug
    """

    # Fetch context objects
    registry = args.ctx.registry
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. General debug function.")

    # ------------------#
    # Function to debug #
    # ------------------#
    # registry.indexdb.idoclinks['Notebook']['Person']['ORG'].horizontal_patch_elasticsearch(actions=('print'))
    # registry.indexdb.doclinks_vertical_patch_all(actions=('eval'))
    # registry.indexdb.doclinks_horizontal_patch_all(actions=('eval', 'commit'))
    # db.print_database_stats(engine_name='xaas_coresrv', schema_name='graphsearch_test'   , re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    # db.print_database_stats(engine_name='xaas_coresrv', schema_name='elasticsearch_cache', re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    # registry.indexdb.idoclinks['Category']['Course']['SEM'].horizontal_patch(actions=('eval', 'commit', 'print'))
    # registry.cachemanager.calculate_scores_matrix(  from_object_type='Category', to_object_type='Category', actions=('print', 'commit'))
    # registry.cachemanager.consolidate_scores_matrix(from_object_type='Category', to_object_type='Category', update_averages=True, score_thr=0.1, actions=('commit'))

    # registry.indexdb.idoclinks['Person']['Publication']['ORG'].horizontal_patch(actions=('eval', 'print'))

    # registry.indexdb.idoclinks['Course']['Person']['ORG'].horizontal_patch_elasticsearch(actions=('eval', 'print', 'commit'))
    # registry.indexdb.create_mixed_views(drop_existing=True, test_mode=False)

    # registry.orchestrator.update_dates(actions=('commit',))

    # db.execute_query_stream_to_file(
    #     engine_name='xaas_coresrv',
    #     query='SELECT * FROM elasticsearch_cache.Index_D_Concept',
    #     schema_name='elasticsearch_cache',
    #     output_file='./output.csv'
    # )

    # print('Atempt 2RVGrr2')
    # registry.indexes.generate_local_cache_streaming( index_date='2026-03-03', ignore_warnings=False, replace_existing=True, force_replace=True)
    # registry.indexes.generate_index_from_local_cache(index_date='2026-03-03', ignore_warnings=False, replace_existing=True, force_replace=True)


    # from graphregistry.common.config import GlobalConfig
    # glbcfg = GlobalConfig()
    # db.print_database_stats(engine_name='xaas_coresrv', schema_name=glbcfg.settings['mysql']['db_schema_names']['graphsearch_test']   , re_exclude=[r'.*(MOOC|Lecture|Widget|Notebook|Exercise|Specialisation|Startup|StudyPlan).*'])
    # db.print_database_stats(engine_name='xaas_coresrv', schema_name=glbcfg.settings['mysql']['db_schema_names']['elasticsearch_cache'], re_exclude=[r'.*(MOOC|Lecture|Widget|Notebook|Exercise|Specialisation|Startup|StudyPlan).*'])


    # registry.indexdb.idoclinks['Category']['Course']['SEM'].horizontal_patch(actions=('print', 'commit'))

    # registry.indexdb.idoclinks['Publication']['Person']['ORG'].vertical_patch_parentchild(actions=('print', 'commit'))

    registry.indexdb.delete_loose_ends(actions=('print', 'eval'))


    # Print footers
    print("🖥️  ~ Done.")
