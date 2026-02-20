import os

#-------------------------------------#
# Handler: Test ElasticSearch servers #
#-------------------------------------#
def cmd_es_test(args):
    """
    Handle:
      graphregistry index test [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Test ElasticSearch server.")

    # Execute command:
    # - Test connection to ElasticSearch server
    if args.env:
        if index.test(engine_name=args.env) is True:
            print(f"✅ ElasticSearch server is up and running [env='{args.env}'].")
        else:
            print(f"❌ ElasticSearch server is down or unreachable [env='{args.env}'].")
    else:
        for engine in index.engine.keys():
            if index.test(engine_name=engine) is True:
                print(f"✅ ElasticSearch server is up and running [env='{engine}'].")
            else:
                print(f"❌ ElasticSearch server is down or unreachable [env='{engine}'].")

    # Print footers
    print("🖥️  ~ Done.")

#----------------------------------------------#
# Handler: Print info on ElasticSearch servers #
#----------------------------------------------#
def cmd_es_info(args):
    """
    Handle:
      graphregistry index info [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Test ElasticSearch server.")

    # Execute command:
    # - Print info on ElasticSearch server
    index.info(engine_name=args.env)

    # Print footers
    print("🖥️  ~ Done.")

#------------------------------------------------#
# Handler: Print health of ElasticSearch servers #
#------------------------------------------------#
def cmd_es_health(args):
    """
    Handle:
      graphregistry index health [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Test ElasticSearch server.")

    # Execute command:
    # - Print health of ElasticSearch server
    index.cluster_health(engine_name=args.env)

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------#
# Handler: List ElasticSearch indexes #
#-------------------------------------#
def cmd_es_list(args):
    """
    Handle:
      graphregistry index list [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. List ElasticSearch indexes.")

    # Execute command:
    # - List ElasticSearch indexes or aliases
    if args.aliases is True:
        index.alias_list(engine_name=args.env)
    else:
        index.index_list(engine_name=args.env, display_size=args.display_size)

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------#
# Handler: Export ElasticSearch index #
#-------------------------------------#
def cmd_es_export(args):
    """
    Handle:
      graphregistry index export [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Export ElasticSearch index.")

    # Execute command:
    # - Export ElasticSearch index to local folder
    index.export_index_to_folder(
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

#-------------------------------------#
# Handler: Import ElasticSearch index #
#-------------------------------------#
def cmd_es_import(args):
    """
    Handle:
      graphregistry index import [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Fetch input parameters
    env              = args.env
    input_folder     = args.input_folder
    rename_to        = args.rename_to
    chunk_size       = args.chunk_size
    replace_existing = args.replace_existing
    force            = args.force

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Import ElasticSearch index.")

    # Execute command:
    # - Import ElasticSearch index from local folder

    if rename_to is None:
        rename_to = os.path.basename(input_folder)

    index.import_index_from_folder(
        engine_name      = env,
        input_folder     = input_folder,
        rename_to        = rename_to,
        chunk_size       = chunk_size,
        replace_existing = replace_existing,
        force            = force
    )

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------#
# Handler: Copy ElasticSearch index   #
#-------------------------------------#
def cmd_es_copy(args):
    """
    Handle:
      graphregistry index copy [...]
    """

    # Fetch context objects
    index = args.ctx.index
    gz = args.use_gzip
    r  = args.replace_existing
    f  = args.force

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Copy ElasticSearch index.")

    # Execute command:
    # - Copy ElasticSearch index from source to destination
    if args.alias_pattern is None:
        index.copy_index_across_engines(
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
        index.copy_aliases_across_engines(
            source_engine    = args.from_env,
            target_engine    = args.to_env,
            alias_pattern    = args.alias_pattern,
            replace_existing = r,
            force            = f
        )

    # Print footers
    print("🖥️  ~ Done.")
