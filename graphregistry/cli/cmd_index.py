import datetime

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_index_build(args):
    """
    Handle:
      graphregistry index build [...]
    """

    # Fetch context objects
    registry = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Build index field tables.")

    # Get input options
    actions = tuple(args.actions.split(',')) if args.actions else ()

    # -----------------#
    # Execute commands #
    # -----------------#
    registry.indexdb.build(actions=actions)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_index_patch(args):
    """
    Handle:
      graphregistry index patch [...]
    """

    # Fetch context objects
    registry = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Patch index field tables.")

    # Get input options
    actions = tuple(args.actions.split(',')) if args.actions else ()

    # -----------------#
    # Execute commands #
    # -----------------#
    registry.indexdb.patch(actions=actions)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_index_mixed_views(args):
    """
    Handle:
      graphregistry index mixed_views [...]
    """

    # Fetch context objects
    registry = args.ctx.registry
    r = args.replace_existing
    t = args.test_mode

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Generate mixed views for ElasticSearch.")

    # -----------------
    registry.indexdb.create_mixed_views(drop_existing=r, test_mode=t)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_index_generate(args):
    """
    Handle:
      graphregistry cache generate [...]
    """

    # Fetch context objects
    registry   = args.ctx.registry
    target     = args.target
    index_date = args.index_date if args.index_date else str(datetime.date.today())
    i   = args.ignore_warnings
    r   = args.replace_existing
    f   = args.force_replace
    lco = args.local_cache_only
    ifo = args.index_file_only

    # Print headers
    print("🖥️  ~ Graph Registry CLI. ...")

    # -----------------#
    # Execute commands #
    # -----------------#

    # Generate ElasticSearch index
    if target=='elasticsearch':

        # Generate local ES cache from MySQL
        if not ifo:
            registry.indexes.generate_local_cache_streaming(index_date=index_date, ignore_warnings=i, replace_existing=r, force_replace=f)

        # Generate ES index file from local cache
        if not lco:
            registry.indexes.generate_index_from_local_cache(index_date=index_date, ignore_warnings=i, replace_existing=r, force_replace=f)

    # Print footers
    print("🖥️  ~ Done.")


