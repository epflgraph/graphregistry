# graphregistry/domain/types.py
from __future__ import annotations
from typing import Literal, TypeAlias

# Define a type for supported field languages, which can be used in custom fields of nodes
TextLanguage  = Literal['en', 'fr', 'de', 'it']
FieldLanguage = Literal['en', 'fr', 'de', 'it', 'n/a']
ObjectType    = Literal['Category', 'Concept', 'Course', 'Curated area', 'Exercise', 'Lecture', 'MOOC', 'Notebook', 'Person', 'Publication', 'Slide', 'Specialisation', 'Startup', 'StudyPlan', 'Unit', 'Widget']

# Define a type for supported action names that can be performed on nodes or edges
ActionName: TypeAlias = Literal['print', 'eval', 'commit']
ActionSet:  TypeAlias = tuple[ActionName, ...]

# Define a type for the subtypes of links between index documents, matching the
# link_subtype column values: organisational links carry the parent-child
# direction, semantic links are concept-based and scored.
LinkSubtype: TypeAlias = Literal['Parent-to-Child', 'Child-to-Parent', 'Semantic']

# Define a type for the partitions of the index doc-link tables: organisational
# links are structural parent-child relations, semantic links are concept-based
# and ranked. The values match the _T_ suffix of the Index_D_*_L_*_T_* tables.
LinkTablePartition: TypeAlias = Literal['ORG', 'SEM']

# Define a type for the flag families tracked by the indexing pipeline
FlagType: TypeAlias = Literal['fields', 'scores']

# Define a type for the domain class of a scores matrix
ScoreDomain: TypeAlias = Literal['education', 'research', 'ontology']

# Define a type for the flavour of a scores matrix. GBC is the group-by-concepts
# matrix of raw concept-overlap scores; AS is the adjusted-scores matrix
# consolidated with rolling averages. The values match the table suffixes.
ScoreMatrixKind: TypeAlias = Literal['GBC', 'AS']

# Define a type for the score column that drives link ranking in index doc-link tables
ScoreType: TypeAlias = Literal['semantic_score', 'degree_score']

# Define a type for the SQL formula families applied during cache materialisation
FormulaType: TypeAlias = Literal['calculated_fields', 'graph_traversals', 'calculated_scores']

# Type alias for language codes
LanguageCode: TypeAlias = str
LanguageCodeList: TypeAlias = tuple[LanguageCode, ...]
DEFAULT_LANGUAGE_CODES: LanguageCodeList = ('en', 'fr', 'de', 'it')
