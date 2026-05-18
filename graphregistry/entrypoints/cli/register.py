# graphregistry/entrypoints/cli/register.py
from graphregistry.entrypoints.cli.specs import cli_definitions

#==============================#
# Register domain and commands #
#==============================#
def register(subparsers, cmd_name):
    f"""
    Register '{cmd_name}' domain commands.

    Usage:
      graphregistry {cmd_name} [-h|...]
    """

    # Register Level 1 parser
    # >> graphregistry cmd_name [-h|...]
    parser = subparsers.add_parser(cmd_name, help=cli_definitions[cmd_name]['help'])

    # Register Level 2 parser
    # >> graphregistry cmd_name cmd [-h|...]
    subcmd_cmd_name = parser.add_subparsers(dest=f"{cmd_name}_cmd", metavar="<command>", required=True,
        help=f"<{cmd_name}> subcommands. Use \"graphregistry {cmd_name} <command> -h\" for options.")

    # Initialize dictionary to hold subcommand parsers
    subcommands = {}

    #-----------------------------------------#
    # Iterate over commands and register them #
    #-----------------------------------------#
    for name, spec in cli_definitions[cmd_name]['commands'].items():

        # Create subcommand parser
        p = subcmd_cmd_name.add_parser(name, help=spec['help'])

        # Add common args
        for arg_name in spec.get('common_args', []):
            arg = cli_definitions[cmd_name]['common_args'][arg_name]
            p.add_argument(*arg['flags'], **arg['kwargs'])

        # Add command-specific args
        for arg in spec.get('args', []):
            p.add_argument(*arg['flags'], **arg['kwargs'])

        # Set command handler
        p.set_defaults(func=spec['func'])
        subcommands[name] = p
