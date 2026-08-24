# graphregistry/domain/models/entities/mdl_lecture.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.entities.mdl_node import Node

# Model definition
class Video(BaseModel):
    """Model representing a lecture video.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    token       : str
    file_url    : str
    fingerprint : str | None = None
    codec       : str | None = None
    duration    : float | None = None
    bit_rate    : int | None = None
    sample_rate : int | None = None
    resolution  : str | None = None

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    @model_validator(mode="after")
    def validate_key_type(self) -> "Video":
        if not isinstance(self.token, str):
            raise TypeError("token must be a string")
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "Video":
        return cls.model_validate(input_json)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

# Model definition
class Voice(BaseModel):
    """Model representing a lecture audio.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    token         : str
    fingerprint   : str   | None = None
    duration      : float | None = None

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    @model_validator(mode="after")
    def validate_key_type(self) -> "Voice":
        if not isinstance(self.token, str):
            raise TypeError("token must be a string")
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "Voice":
        return cls.model_validate(input_json)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

# Model definition
class Slide(BaseModel):
    """Model representing a lecture slide (or keyframe).
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    token         : str
    timestamp     : int
    fingerprint   : str | None = None
    text          : str | None = None
    language      : str | None = None
    translations  : dict[str, str] | None = None

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    @model_validator(mode="after")
    def validate_key_type(self) -> "Slide":
        if not isinstance(self.token, str):
            raise TypeError("token must be a string")
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "Slide":
        return cls.model_validate(input_json)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

# Model definition
class SlideList(BaseModel):
    """Model representing a list of lecture slides (or keyframes).
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[Slide] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_list(cls, input_list: list[dict[str, Any]]) -> "SlideList":
        return cls(item_list=[Slide.model_validate(doc) for doc in (input_list or [])])

    def to_list(self) -> list[dict[str, Any]]:
        return self.model_dump(mode='json')['item_list']

# Model definition
class TranscriptSegment(BaseModel):
    """Model representing a segment of lecture transcript.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    start         : float
    end           : float
    text          : str
    translations  : dict[str, str] | None = None

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "TranscriptSegment":
        return cls.model_validate(input_json)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

# Model definition
class Transcript(BaseModel):
    """Model representing a lecture transcript.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    language  : str | None = None
    full_text : str | None = None
    item_list : list[TranscriptSegment] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "Transcript":
        return cls(
            language=str(input_json.get("language", "")),
            full_text=str(input_json.get("full_text", "")) if input_json.get("full_text") else None,
            item_list=[TranscriptSegment.model_validate(seg) for seg in (input_json.get("segments") or [])]
        )

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

# Model definition
class Lecture(BaseModel):
    """Model representing a lecture, which may include video, audio, slides, and transcript.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    node       : Node
    video      : Video      | None = None
    voice      : Voice      | None = None
    slides     : SlideList  | None = None
    transcript : Transcript | None = None

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "Lecture":
        return cls(
            node       =       Node.from_json(input_json.get("node",       {})),
            video      =      Video.from_json(input_json.get("video",      {})),
            voice      =      Voice.from_json(input_json.get("voice",      {})) if input_json.get("voice")      else None,
            slides     =  SlideList.from_list(input_json.get("slides",     [])) if input_json.get("slides")     else None,
            transcript = Transcript.from_json(input_json.get("transcript", {})) if input_json.get("transcript") else None
        )

# Model definition
class LectureList(BaseModel):
    """Model representing a list of lectures.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[Lecture] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_list(cls, input_list: list[dict[str, Any]]) -> "LectureList":
        return cls(item_list=[Lecture.from_json(doc) for doc in (input_list or [])])

    def to_list(self) -> list[dict[str, Any]]:
        return self.model_dump(mode='json')['item_list']
