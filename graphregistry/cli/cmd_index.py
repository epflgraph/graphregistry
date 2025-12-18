
#-------------------------------------#
# Handler: Test ElasticSearch servers #
#-------------------------------------#
def cmd_index_test(args):
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
    if index.test(engine_name=args.env) is True:
        print(f"✅ ElasticSearch server is up and running [env='{args.env}'].")
    else:
        print(f"❌ ElasticSearch server is down or unreachable [env='{args.env}'].")

    # Print footers
    print("🖥️  ~ Done.")

#----------------------------------------------#
# Handler: Print info on ElasticSearch servers #
#----------------------------------------------#
def cmd_index_info(args):
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
def cmd_index_health(args):
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
def cmd_index_list(args):
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
def cmd_index_export(args):
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
def cmd_index_import(args):
    """
    Handle:
      graphregistry index import [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Import ElasticSearch index.")

    # Execute command:
    # - Import ElasticSearch index from local folder
    index.import_index_from_folder(
        engine_name      = args.env,
        input_folder     = args.input_folder,
        rename_to        = args.rename_to,
        chunk_size       = args.chunk_size,
        replace_existing = args.replace_existing,
        force            = args.force
    )

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------#
# Handler: Copy ElasticSearch index   #
#-------------------------------------#
def cmd_index_copy(args):
    """
    Handle:
      graphregistry index copy [...]
    """

    # Fetch context objects
    index = args.ctx.index

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Copy ElasticSearch index.")

    # Execute command:
    # - Copy ElasticSearch index from source to destination
    # index.copy_index_across_engines(
    #     source_engine_name = args.from_env,
    #     target_engine_name = args.to_env,
    #     index_name = args.index_name,
    #     rename_to  = args.rename_to,
    #     chunk_size = args.chunk_size
    # )
    index.copy_index_across_engines(
        index_name       = args.index_name,
        source_engine    = args.from_env,
        target_engine    = args.to_env,
        rename_to        = args.rename_to,
        chunk_size       = args.chunk_size,
        use_gzip         = args.use_gzip,
        replace_existing = args.replace_existing,
        force            = args.force
    )

    # Print footers
    print("🖥️  ~ Done.")
