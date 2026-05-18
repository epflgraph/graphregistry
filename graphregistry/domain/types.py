# graphregistry/domain/types.py
from __future__ import annotations
from typing import Literal, TypeAlias

# Define a type for supported field languages, which can be used in custom fields of nodes
TextLanguage  = Literal['en', 'fr', 'de', 'it']
FieldLanguage = Literal['en', 'fr', 'de', 'it', 'n/a']
ObjectType    = Literal['Category', 'Concept', 'Course', 'Exercise', 'Lecture', 'MOOC', 'Notebook', 'Person', 'Publication', 'Specialisation', 'Startup', 'StudyPlan', 'Unit', 'Widget']

# Define a type for supported action names that can be performed on nodes or edges
ActionName: TypeAlias = Literal['print', 'eval', 'commit']
ActionSet:  TypeAlias = tuple[ActionName, ...]

# Type alias for language codes
LanguageCode: TypeAlias = str
LanguageCodeList: TypeAlias = tuple[LanguageCode, ...]
DEFAULT_LANGUAGE_CODES: LanguageCodeList = ('en', 'fr', 'de', 'it')
