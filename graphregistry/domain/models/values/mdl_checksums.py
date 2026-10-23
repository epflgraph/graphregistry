# graphregistry/domain/models/values/mdl_checksums.py
from __future__ import annotations
import hashlib
from pydantic import BaseModel

# Sentinel substituted for missing checksum components when composites are computed.
# It must match the COALESCE sentinel of the SQL checksum queries so that composites
# computed in Python and in SQL agree byte for byte.
NULL_SENTINEL = "__null__"

#==================#
# Class Definition #
#==================#
class Checksum(BaseModel):
    """Model representing an immutable checksum (MD5 hex digest) computed over registry
    content. Checksums are the drift-detection mechanism of the indexing pipeline: two
    different digests for the same tracked item mean the derived caches are out of date.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Checksums are value objects: equal digests are interchangeable and never mutate.
    model_config = {"frozen": True}
    value: str

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    # Public Method: Build a checksum from a raw string, mirroring the SQL MD5 function.
    @classmethod
    def from_string(cls, input_string: str) -> "Checksum":
        return cls(value=hashlib.md5(input_string.encode("utf-8")).hexdigest())

#==================#
# Class Definition #
#==================#
class ChecksumPair(BaseModel):
    """Model representing the current and previous checksums of one tracked item. The
    pair encodes the drift window of the pipeline: 'previous' is the digest of the last
    processed state, 'current' is the digest of the latest registry state.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # The pair is rebuilt rather than mutated whenever a lifecycle step advances it.
    model_config = {"frozen": True}
    current          : Checksum | None = None
    previous         : Checksum | None = None

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Report whether drift was detected, following the SQL convention
    # that a missing checksum on either side counts as drift unless both are missing.
    @property
    def has_changed(self) -> bool:
        if self.current is None and self.previous is None:
            return False
        if self.current is None or self.previous is None:
            return True
        return self.current.value != self.previous.value

    #---------------------#
    # Lifecycle methods #
    #---------------------#
    # Public Method: Close the drift window by adopting the current checksum as the
    # new previous one, mirroring the legacy rollover command.
    def rollover(self) -> "ChecksumPair":
        return ChecksumPair(current=self.current, previous=self.current)

#==================#
# Class Definition #
#==================#
class ChecksumBundle(BaseModel):
    """Model representing the three partial checksums maintained per tracked object:
    core object attributes, page profile, and custom fields. The final object checksum
    is the MD5 digest of their concatenation, replicating the composite rule of the
    legacy checksum queries.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    object_checksum        : Checksum | None = None
    page_profile_checksum  : Checksum | None = None
    custom_fields_checksum : Checksum | None = None

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Compute the final composite checksum of the bundle. Missing
    # components are replaced by the null sentinel so the result matches the
    # database-computed value; an entirely empty bundle has no composite.
    @property
    def composite(self) -> Checksum | None:
        # An object that was never checksummed has no composite to offer.
        if (self.object_checksum is None
                and self.page_profile_checksum is None
                and self.custom_fields_checksum is None):
            return None
        # Concatenate the components with null sentinels, as the SQL CONCAT/COALESCE does.
        parts = [
            checksum.value if checksum is not None else NULL_SENTINEL
            for checksum in (
                self.object_checksum,
                self.page_profile_checksum,
                self.custom_fields_checksum,
            )
        ]
        return Checksum.from_string("".join(parts))
