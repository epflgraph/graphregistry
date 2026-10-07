# graphregistry/application/operations/ops_integrity.py
from __future__ import annotations
from graphregistry.domain.types import ObjectType

# Literal for fields to be checked for corruption
CorruptionCheckFields = Literal['name', 'description', 'translations', 'url']

# Class definition
class OutputIntegrityOperations:

    # Class constructor
    def __init__(self) -> None:
        pass

    # Method: Count number of null fields in content tables
    def count_null_fields(self) -> None:
        print('Not implemented.')
        pass

    # Method: Scan and detect content fields that are likely to be corrupted
    def scan_for_corrupted_fields(self, object_type: ObjectType, fields_to_check: list[CorruptionCheckFields]) -> None:
        print('Not implemented.')
        pass
