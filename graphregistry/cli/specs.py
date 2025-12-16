from typing import Any, Dict
from graphregistry.cli.cmd_airflow import (
    cmd_airflow_sync,
    cmd_airflow_status,
    cmd_airflow_to_process,
    cmd_airflow_config,
    cmd_airflow_expire
)
from graphregistry.cli.cmd_cache import (
    cmd_cache_to_process
)
from graphregistry.cli.cmd_db import (
    cmd_db_test
)
from graphregistry.cli.cmd_index import (
    cmd_index_test,
    cmd_index_info,
    cmd_index_health,
    cmd_index_list,
)

# ==================================================#
# CLI Definitions for all Subcommands and Arguments #
# ==================================================#
cli_definitions: Dict[str, Any] = {
    # --------------------#
    # Domain: index       #
    # --------------------#
    'index': dict(
        help = "Manage ElasticSearch server and indexes.",
        common_args = {
            'env': dict(
                flags = ('--env',),
                kwargs = dict(
                    help = "Specify environment (default=test).",
                    choices = ('test', 'prod'),
                    default = 'test'
                )
            )
        },
        commands = {
            'test': dict(
                help = "Test ElasticSearch server(s).",
                func = cmd_index_test,
                common_args = ['env'],
            ),
            'info': dict(
                help = "Print server info.",
                func = cmd_index_info,
                common_args = ['env'],
            ),
            'health': dict(
                help = "Print server health.",
                func = cmd_index_health,
                common_args = ['env'],
            ),
            'list': dict(
                help = "List indexes.",
                func = cmd_index_list,
                common_args = ['env'],
                args = [dict(flags = ('--display_size', '-s'), kwargs = dict(action='store_true')),
                        dict(flags = ('--alias'       , '-a'), kwargs = dict(action='store_true'))
                ]
            )
        }
    ),

    # --------------------#
    # Domain: airflow     #
    # --------------------#
    "airflow": dict(
        help="Synchronize Registry with Airflow and manage type-flag configurations.",
        common_args=dict(),  # (none for now)
        commands=dict(
            sync=dict(
                help="Sync registry with Airflow",
                func=cmd_airflow_sync,
            ),
            status=dict(
                help="Get status from Airflow",
                func=cmd_airflow_status,
            ),
            to_process=dict(
                help="Operations on the 'to process' queue for airflow jobs.",
                func=cmd_airflow_to_process,
                # NOTE: mutually exclusive group not representable in the simple spec;
                # we model as plain args here.
                args=[
                    dict(flags=("--count",), kwargs=dict(action="store_true", help="Show number of items waiting to be processed.")),
                    dict(flags=("--reset",), kwargs=dict(action="store_true", help="Reset the 'to process' queue / counters.")),
                ],
            ),
            config=dict(
                help="Configure Airflow typeflags for orchestration.",
                func=cmd_airflow_config,
                args=[
                    dict(
                        flags=("--typeflags",),
                        kwargs=dict(
                            required=True,
                            type=str,
                            help="Typeflags configuration as a JSON string, or '@path/to/file.json' to load JSON from a file.",
                        ),
                    )
                ],
            ),
            expire=dict(
                help="Set 'has_expired' flag to 1 for objects based on date when they were last cached.",
                func=cmd_airflow_expire,
                args=[
                    dict(flags=("--object_type",), kwargs=dict(required=False, type=str, help="Process only the input object type (default=all).")),
                    dict(flags=("--older_than",), kwargs=dict(required=False, type=int, help="Set 'has_expired' flag to 1 for objects older than <int> in days (default=90).")),
                    dict(flags=("--limit_per_type",), kwargs=dict(required=False, type=int, help="Limit number of objects to process (default=100).")),
                    dict(flags=("--verbose",), kwargs=dict(action="store_true", help="Execute in verbose mode.")),
                ],
            ),
        ),
    ),

    # --------------------#
    # Domain: cache       #
    # --------------------#
    "cache": dict(
        help="Cache-related operations (pending items, recalculation, etc.).",
        common_args=dict(),  # (none for now)
        commands=dict(
            to_process=dict(
                help="Operations on the 'to process' queue for cache jobs.",
                func=cmd_cache_to_process,
                # NOTE: mutually exclusive group not representable in the simple spec;
                # we model as plain args here.
                args=[
                    dict(flags=("--count",), kwargs=dict(action="store_true", help="Show number of items waiting to be processed.")),
                    dict(flags=("--reset",), kwargs=dict(action="store_true", help="Reset the 'to process' queue / counters.")),
                ],
            ),
        ),
    ),

    # --------------------#
    # Domain: db          #
    # --------------------#
    "db": dict(
        help="MySQL database client.",
        common_args=dict(),  # (none for now)
        commands=dict(
            test=dict(
                help="Test MySQL connection.",
                func=cmd_db_test,
            )
        ),
    ),
}
