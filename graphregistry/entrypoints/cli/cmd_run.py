
#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_run_formula(args):

    # Fetch context objects
    registry = args.ctx.registry
    glbcfg   = args.ctx.global_config

    # Get input options
    input_file   = args.input
    resolve_only = args.resolve_only
    verbose      = args.verbose

    # -----------------#
    # Execute commands #
    # -----------------#

    # Open and read SQL formula from file
    if input_file is not None:
        with open(input_file, 'r') as f:
            sql_formula = f.read()
    else:
        print("No input file provided. Exiting.")
        return

    # Fill in the template variables
    for db_schema_name in glbcfg.mysql_schema_names['xaas_coresrv']:
        sql_formula = sql_formula.replace(f'[[{db_schema_name}]]', glbcfg.mysql_schema_names['xaas_coresrv'][db_schema_name])

    # Print the final SQL if resolve_only is True
    if resolve_only or verbose:
        print("Resolved SQL formula:")
        print(sql_formula)
        return