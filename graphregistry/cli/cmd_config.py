
#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_config_index(args):
    """
    Handle:
      graphregistry config index
    """

    # Fetch context objects
    idxcfg = args.ctx.index_config

    # Print headers
    print("🖥️  ~ Graph Registry CLI. ...")

    # Print out config
    idxcfg.print(compact=True)

    # Print footers
    print("🖥️  ~ Done.")
