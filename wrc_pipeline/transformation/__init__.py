"""Transformation stage: Landing Zone -> cleaned, identifier-named transformed documents.

Reads only from MongoDB landing metadata and the landing object store. The Landing Zone is
immutable and is never written to from here.
"""

# Bump this when the transformation algorithm changes. A new value produces a new
# transformed version of every landing document without touching the old ones, because it
# is part of the transformed record's unique key.
TRANSFORMATION_VERSION = 1

__all__ = ["TRANSFORMATION_VERSION"]
