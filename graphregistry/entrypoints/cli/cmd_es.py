# graphregistry/entrypoints/cli/cmd_es.py
import os

#-----------------------------------#
# Handler: Test server connectivity #
#-----------------------------------#
def cmd_es_test(args):
    """
    Usage:
        graphregistry es test [...]
    """

    # Fetch context objects
    es = args.ctx.es

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Test server connectivity.")

    # Execute command:
    # - Test connection to ElasticSearch server
    if args.env:
        if es.test(engine_name=args.env) is True:
            print(f"✅ ElasticSearch server is up and running [env='{args.env}'].")
        else:
            print(f"❌ ElasticSearch server is down or unreachable [env='{args.env}'].")
    else:
        for engine in es.engine.keys():
            if es.test(engine_name=engine) is True:
                print(f"✅ ElasticSearch server is up and running [env='{engine}'].")
            else:
                print(f"❌ ElasticSearch server is down or unreachable [env='{engine}'].")

    # Print footers
    print("🖥️  ~ Done.")

#----------------------------#
# Handler: Print server info #
#----------------------------#
def cmd_es_info(args):
    """
    Usage:
        graphregistry es info [...]
    """

    # Fetch context objects
    es = args.ctx.es

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Print server info.")

    # Execute command:
    # - Print info on ElasticSearch server
    es.info(engine_name=args.env)

    # Print footers
    print("🖥️  ~ Done.")

#------------------------------#
# Handler: Print server health #
#------------------------------#
def cmd_es_health(args):
    """
    Usage:
        graphregistry es health [...]
    """

    # Fetch context objects
    es = args.ctx.es

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Print server health.")

    # Execute command:
    # - Print health of ElasticSearch server
    es.cluster_health(engine_name=args.env)

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------#
# Handler: List indexes on the server #
#-------------------------------------#
def cmd_es_list(args):
    """
    Usage:
        graphregistry es list [...]
    """

    # Fetch context objects
    es = args.ctx.es

    # Print headers
    print("🖥️  ~ Graph Registry CLI. List indexes on the server.")

    # Execute command:
    # - List ElasticSearch indexes or aliases
    if args.aliases is True:
        es.alias_list(engine_name=args.env)
    else:
        es.index_list(engine_name=args.env, display_size=args.display_size)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Export index into local folder #
#-----------------------------------------#
def cmd_es_export(args):
    """
    Usage:
        graphregistry es export [...]
    """

    # Fetch context objects
    es = args.ctx.es

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Export index into local folder.")

    # Execute command:
    # - Export ElasticSearch index to local folder
    es.export_index_to_folder(
        engine_name      = args.env,
        index_name       = args.index_name,
        output_folder    = args.output_folder,
        chunk_size       = args.chunk_size,
        use_gzip         = args.use_gzip,
        replace_existing = args.replace_existing,
        force            = args.force
    )

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Import index from local folder #
#-----------------------------------------#
def cmd_es_import(args):
    """
    Usage:
        graphregistry es import [...]
    """

    # Fetch context objects
    es = args.ctx.es

    # Fetch input parameters
    env              = args.env
    input_folder     = args.input_folder
    rename_to        = args.rename_to
    chunk_size       = args.chunk_size
    replace_existing = args.replace_existing
    force            = args.force

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Import index from local folder.")

    # Execute command:
    # - Import ElasticSearch index from local folder

    if rename_to is None:
        rename_to = os.path.basename(input_folder)

    es.import_index_from_folder(
        engine_name      = env,
        input_folder     = input_folder,
        rename_to        = rename_to,
        chunk_size       = chunk_size,
        replace_existing = replace_existing,
        force            = force
    )

    # Print footers
    print("🖥️  ~ Done.")

#------------------------------------#
# Handler: Copy index across servers #
#------------------------------------#
def cmd_es_copy(args):
    """
    Usage:
        graphregistry es copy [...]
    """

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Copy index across servers.")

    # Fetch context objects
    es = args.ctx.es
    gz = args.use_gzip
    r  = args.replace_existing
    f  = args.force

    # ...
    if args.alias_pattern is None:
        es.copy_index_across_engines(
            index_name       = args.index_name,
            source_engine    = args.from_env,
            target_engine    = args.to_env,
            rename_to        = args.rename_to,
            chunk_size       = args.chunk_size,
            use_gzip         = gz,
            replace_existing = r,
            force            = f
        )
    else:
        es.copy_aliases_across_engines(
            source_engine    = args.from_env,
            target_engine    = args.to_env,
            alias_pattern    = args.alias_pattern,
            replace_existing = r,
            force            = f
        )

    # Print footers
    print("🖥️  ~ Done.")

#---------------------------------#
# Handler: Operations on an index #
#---------------------------------#
def cmd_es_index(args):
    """
    Usage:
        graphregistry es index [...]
    """

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Operations on an index.")

    # Fetch context objects
    es = args.ctx.es
    engine_name = args.env
    index_name  = args.index_name
    alias_name  = args.create_alias
    # r  = args.replace_existing
    # f  = args.force

    es.set_alias(engine_name=engine_name, alias_name=alias_name, index_name=index_name)

    # Print footers
    print("🖥️  ~ Done.")



# TODO: Keep correcting comments and descriptions
# Also address the following:

# explain
#  SELECT *
#    FROM graph_airflow.Operations_N_Object_T_FieldsChanged
#   WHERE (object_type, object_id) NOT IN (SELECT object_type, object_id FROM graph_registry.Nodes_N_Object)
#     AND (object_type, object_id) NOT IN (SELECT object_type, object_id FROM graph_lectures.Nodes_N_Object)
#     AND (object_type, object_id) NOT IN (SELECT object_type, object_id FROM graph_ontology.Nodes_N_Object);
 
#  explain
#  SELECT *
# FROM graph_airflow.Operations_N_Object_T_FieldsChanged o
# WHERE NOT EXISTS (
#         SELECT 1
#         FROM graph_registry.Nodes_N_Object r
#         WHERE r.object_type = o.object_type
#           AND r.object_id   = o.object_id
#       )
#   AND NOT EXISTS (
#         SELECT 1
#         FROM graph_lectures.Nodes_N_Object l
#         WHERE l.object_type = o.object_type
#           AND l.object_id   = o.object_id
#       )
#   AND NOT EXISTS (
#         SELECT 1
#         FROM graph_ontology.Nodes_N_Object g
#         WHERE g.object_type = o.object_type
#           AND g.object_id   = o.object_id
#       );
      
#       explain
# SELECT *
# FROM graph_airflow.Operations_N_Object_T_FieldsChanged o
# LEFT JOIN graph_registry.Nodes_N_Object r
#   ON r.object_type = o.object_type AND r.object_id = o.object_id
# LEFT JOIN graph_lectures.Nodes_N_Object l
#   ON l.object_type = o.object_type AND l.object_id = o.object_id
# LEFT JOIN graph_ontology.Nodes_N_Object g
#   ON g.object_type = o.object_type AND g.object_id = o.object_id
# WHERE r.object_id IS NULL
#   AND l.object_id IS NULL
#   AND g.object_id IS NULL;
 