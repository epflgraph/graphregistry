# Graph Registry CLI: index
import json
from pathlib import Path

#===========================#
# Command handler functions #
#===========================#

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
    if args.alias is True:
        index.alias_list(engine_name=args.env)
    else:
        index.index_list(engine_name=args.env, display_size=args.display_size)

    # Print footers
    print("🖥️  ~ Done.")
