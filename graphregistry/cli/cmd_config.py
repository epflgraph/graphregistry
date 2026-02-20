# graphregistry/cli/cmd_config.py
# Inspect and validate Registry configuration files.

#---------------------------------#
# Handler: Print out index config #
#---------------------------------#
def cmd_config_index(args):
    """
    Usage:
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
