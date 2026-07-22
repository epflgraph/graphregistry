# graphregistry/adapters/persistence/mysql/mappers/amp_pageprofile.py
from __future__ import annotations

from typing import Any

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile


class MySQLPageProfileMapper:
    """
    Maps between the domain PageProfile model and the flattened
    Data_N_Object_T_PageProfile MySQL row shape.
    """

    LANGUAGES: tuple[str, ...] = ("en", "fr", "de", "it")
    GENERATED_ATTRS: tuple[str, ...] = (
        "is_auto_generated",
        "is_auto_corrected",
        "is_auto_translated",
        "translated_from",
        "value",
    )
    DESCRIPTION_SIZES: tuple[str, ...] = ("short", "medium", "long")

    @classmethod
    def from_row(cls, row: dict[str, Any] | None, node_key: NodeKey) -> PageProfile:
        """
        Build a domain PageProfile from a flattened database row dict.
        Missing / None row returns a default PageProfile.
        """
        profile = PageProfile(key=node_key)

        if not row:
            return profile

        profile.short_code = str(row.get("short_code") or "")

        if "is_visible" in row and row.get("is_visible") is not None:
            profile.is_visible = bool(row["is_visible"])

        for lang in cls.LANGUAGES:
            numeric_id_key = f"numeric_id_{lang}"
            subtype_key = f"subtype_{lang}"
            external_key_key = f"external_key_{lang}"
            external_url_key = f"external_url_{lang}"

            if numeric_id_key in row and row[numeric_id_key] is not None:
                profile.numeric_id.set(lang, str(row[numeric_id_key]))

            if subtype_key in row and row[subtype_key] is not None:
                profile.subtype.set(lang, str(row[subtype_key]))

            if external_key_key in row and row[external_key_key] is not None:
                profile.external_key.set(lang, str(row[external_key_key]))

            if external_url_key in row and row[external_url_key] is not None:
                profile.external_url.set(lang, str(row[external_url_key]))

        for lang in cls.LANGUAGES:
            updates: dict[str, Any] = {}

            for attr in cls.GENERATED_ATTRS:
                flat_key = f"name_{lang}_{attr}"
                if flat_key not in row or row[flat_key] is None:
                    continue

                value = row[flat_key]
                if attr.startswith("is_auto_"):
                    updates[attr] = bool(value)
                elif attr == "translated_from":
                    updates[attr] = str(value) if value else None
                else:
                    updates[attr] = str(value or "")

            if updates:
                current = profile.name.get(lang)
                profile.name[lang] = current.model_copy(update=updates)

        for size in cls.DESCRIPTION_SIZES:
            size_obj = getattr(profile.description, size)

            for lang in cls.LANGUAGES:
                updates: dict[str, Any] = {}

                for attr in cls.GENERATED_ATTRS:
                    flat_key = f"description_{size}_{lang}_{attr}"
                    if flat_key not in row or row[flat_key] is None:
                        continue

                    value = row[flat_key]
                    if attr.startswith("is_auto_"):
                        updates[attr] = bool(value)
                    elif attr == "translated_from":
                        updates[attr] = str(value) if value else None
                    else:
                        updates[attr] = str(value or "")

                if updates:
                    current = size_obj.get(lang)
                    size_obj[lang] = current.model_copy(update=updates)

        return profile

    @classmethod
    def to_row(cls, profile: PageProfile) -> dict[str, Any]:
        """
        Flatten a domain PageProfile into a dict suitable for
        Data_N_Object_T_PageProfile upsert/update columns.

        Identity columns are intentionally omitted:
        object_type, object_id
        """
        row: dict[str, Any] = {
            "short_code": profile.short_code,
            "is_visible": int(profile.is_visible),
            "record_deleted": 0,
        }

        for lang in cls.LANGUAGES:
            row[f"numeric_id_{lang}"] = cls._empty_to_none(profile.numeric_id.get(lang))
            row[f"subtype_{lang}"] = cls._empty_to_none(profile.subtype.get(lang))
            row[f"external_key_{lang}"] = cls._empty_to_none(profile.external_key.get(lang))
            row[f"external_url_{lang}"] = cls._empty_to_none(profile.external_url.get(lang))

            name_lang_obj = profile.name.get(lang)
            for attr in cls.GENERATED_ATTRS:
                value = getattr(name_lang_obj, attr)
                row[f"name_{lang}_{attr}"] = cls._normalize_generated_attr(attr, value)

            for size in cls.DESCRIPTION_SIZES:
                desc_lang_obj = getattr(profile.description, size).get(lang)
                for attr in cls.GENERATED_ATTRS:
                    value = getattr(desc_lang_obj, attr)
                    row[f"description_{size}_{lang}_{attr}"] = cls._normalize_generated_attr(attr, value)

        return {
            key: value
            for key, value in sorted(row.items())
            if value not in (None, "")
        }

    @staticmethod
    def _empty_to_none(value: Any) -> Any:
        if value == "":
            return None
        return value

    @staticmethod
    def _normalize_generated_attr(attr: str, value: Any) -> Any:
        if attr.startswith("is_auto_"):
            return int(bool(value))
        if attr == "translated_from":
            return value or None
        if value == "":
            return None
        return value