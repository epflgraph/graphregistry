# tests/unit_tests/adapters/persistence/mysql/mappers/test_amp_pageprofile.py
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.mdl_base import NodeKey


ROW_FIXTURE = {
    "institution_id": "EPFL",
    "object_type": "Unit",
    "object_id": "LCAV",
    "numeric_id_en": 10434,
    "numeric_id_fr": 10434,
    "numeric_id_de": None,
    "numeric_id_it": None,
    "short_code": "LCAV",
    "subtype_en": "Laboratory",
    "subtype_fr": "Laboratoire",
    "subtype_de": None,
    "subtype_it": None,
    "name_en_is_auto_generated": 0,
    "name_en_is_auto_corrected": 0,
    "name_en_is_auto_translated": 0,
    "name_en_translated_from": None,
    "name_en_value": "Audiovisual Communications Laboratory",
    "name_fr_is_auto_generated": 0,
    "name_fr_is_auto_corrected": 0,
    "name_fr_is_auto_translated": 0,
    "name_fr_translated_from": "en",
    "name_fr_value": "Laboratoire de communications audiovisuelles",
    "name_de_is_auto_generated": None,
    "name_de_is_auto_corrected": None,
    "name_de_is_auto_translated": None,
    "name_de_translated_from": None,
    "name_de_value": None,
    "name_it_is_auto_generated": None,
    "name_it_is_auto_corrected": None,
    "name_it_is_auto_translated": None,
    "name_it_translated_from": None,
    "name_it_value": None,
    "description_short_en_is_auto_generated": 1,
    "description_short_en_is_auto_corrected": 1,
    "description_short_en_is_auto_translated": 0,
    "description_short_en_translated_from": None,
    "description_short_en_value": "Laboratory active in computational geometry, functional analysis and signal processing.",
    "description_short_fr_is_auto_generated": 1,
    "description_short_fr_is_auto_corrected": 1,
    "description_short_fr_is_auto_translated": 1,
    "description_short_fr_translated_from": "en",
    "description_short_fr_value": "Laboratoire actif dans la géométrie computationnelle, l'analyse fonctionnelle et le traitement des signaux.",
    "description_short_de_is_auto_generated": None,
    "description_short_de_is_auto_corrected": None,
    "description_short_de_is_auto_translated": None,
    "description_short_de_translated_from": None,
    "description_short_de_value": None,
    "description_short_it_is_auto_generated": None,
    "description_short_it_is_auto_corrected": None,
    "description_short_it_is_auto_translated": None,
    "description_short_it_translated_from": None,
    "description_short_it_value": None,
    "description_medium_en_is_auto_generated": 1,
    "description_medium_en_is_auto_corrected": 1,
    "description_medium_en_is_auto_translated": 0,
    "description_medium_en_translated_from": None,
    "description_medium_en_value": "Active in computational geometry, functional analysis and signal processing. EPFL's Laboratory of AudioVisual Communications (LCAV) conducts cutting-edge research in signal processing, audio processing, and image and video processing, following the Reproducible Research philosophy.",
    "description_medium_fr_is_auto_generated": 1,
    "description_medium_fr_is_auto_corrected": 1,
    "description_medium_fr_is_auto_translated": 1,
    "description_medium_fr_translated_from": "en",
    "description_medium_fr_value": "Active dans la géométrie computationnelle, l'analyse fonctionnelle et le traitement des signaux. Le Laboratoire d'AudioVisual Communications (LCAV) de l'EPFL mène des recherches de pointe sur le traitement des signaux, le traitement audio et le traitement des images et des vidéos, conformément à la philosophie Reproductible Research.",
    "description_medium_de_is_auto_generated": None,
    "description_medium_de_is_auto_corrected": None,
    "description_medium_de_is_auto_translated": None,
    "description_medium_de_translated_from": None,
    "description_medium_de_value": None,
    "description_medium_it_is_auto_generated": None,
    "description_medium_it_is_auto_corrected": None,
    "description_medium_it_is_auto_translated": None,
    "description_medium_it_translated_from": None,
    "description_medium_it_value": None,
    "description_long_en_is_auto_generated": 1,
    "description_long_en_is_auto_corrected": 1,
    "description_long_en_is_auto_translated": 0,
    "description_long_en_translated_from": None,
    "description_long_en_value": "The Laboratory of AudioVisual Communications (LCAV) at EPFL, part of the School of Computer and Communication Sciences, focuses on signal processing research, teaching, and technology transfer. Research areas include mathematical signal processing, audio processing, spatial signal processing, and image and video processing. LCAV follows the Reproducible Research philosophy, ensuring all papers, source code, and data sets are available for download. The lab's work in mathematical signal processing explores new sampling techniques for sparse signals, signal representations, and linear time-frequency analysis methods. In audio processing, LCAV researches 3D-audio, room acoustics, sound perception, and spatial audio coding. The image and video processing group works on image acquisition, modeling, multi-view imaging, and augmented reality.",
    "description_long_fr_is_auto_generated": 1,
    "description_long_fr_is_auto_corrected": 1,
    "description_long_fr_is_auto_translated": 1,
    "description_long_fr_translated_from": "en",
    "description_long_fr_value": "Le Laboratoire des communications audiovisuelles de l'EPFL, qui fait partie de l'École des sciences informatiques et de la communication, se concentre sur la recherche, l'enseignement et le transfert de technologie dans le domaine du traitement des signaux. Les domaines de recherche comprennent le traitement mathématique des signaux, le traitement audio, le traitement des signaux spatiaux et le traitement des images et des vidéos. LCAV suit la philosophie de la recherche reproductible, s'assurant que tous les documents, le code source et les ensembles de données sont disponibles en téléchargement. Le travail du laboratoire dans le traitement mathématique des signaux explore de nouvelles techniques d'échantillonnage pour les signaux clairs, les représentations des signaux et les méthodes d'analyse linéaire des fréquences temporelles. Dans le traitement audio, LCAV recherche l'audio en 3D, l'acoustique de la pièce, la perception du son et le codage audio spatial. Le groupe de traitement d'images et de vidéos travaille sur l'acquisition d'images, la modélisation, l'imagerie multi-vues et la réalité augmentée.",
    "description_long_de_is_auto_generated": None,
    "description_long_de_is_auto_corrected": None,
    "description_long_de_is_auto_translated": None,
    "description_long_de_translated_from": None,
    "description_long_de_value": None,
    "description_long_it_is_auto_generated": None,
    "description_long_it_is_auto_corrected": None,
    "description_long_it_is_auto_translated": None,
    "description_long_it_translated_from": None,
    "description_long_it_value": None,
    "external_key_en": "10434",
    "external_key_fr": "10434",
    "external_key_de": None,
    "external_key_it": None,
    "external_url_en": "https://lcav.epfl.ch/",
    "external_url_fr": "https://lcav.epfl.ch/",
    "external_url_de": None,
    "external_url_it": None,
    "is_visible": 1,
}


def make_key() -> NodeKey:
    return NodeKey(
        institution_id="EPFL",
        object_type="Unit",
        object_id="LCAV",
    )


def test_from_row_maps_mysql_row_to_page_profile() -> None:
    key = make_key()

    profile = MySQLPageProfileMapper.from_row(ROW_FIXTURE, key=key)

    assert profile.key == key
    assert profile.short_code == "LCAV"
    assert profile.is_visible is True

    assert profile.numeric_id.en == "10434"
    assert profile.numeric_id.fr == "10434"
    assert profile.numeric_id.de == ""
    assert profile.numeric_id.it == ""

    assert profile.subtype.en == "Laboratory"
    assert profile.subtype.fr == "Laboratoire"
    assert profile.subtype.de == ""
    assert profile.subtype.it == ""

    assert profile.name.en.value == "Audiovisual Communications Laboratory"
    assert profile.name.en.is_auto_generated is False
    assert profile.name.en.is_auto_corrected is False
    assert profile.name.en.is_auto_translated is False
    assert profile.name.en.translated_from is None

    assert profile.name.fr.value == "Laboratoire de communications audiovisuelles"
    assert profile.name.fr.is_auto_generated is False
    assert profile.name.fr.is_auto_corrected is False
    assert profile.name.fr.is_auto_translated is False
    assert profile.name.fr.translated_from == "en"

    assert profile.name.de.value == ""
    assert profile.name.de.translated_from is None

    assert profile.description.short.en.value.startswith("Laboratory active in computational geometry")
    assert profile.description.short.en.is_auto_generated is True
    assert profile.description.short.en.is_auto_corrected is True
    assert profile.description.short.en.is_auto_translated is False

    assert profile.description.short.fr.value.startswith("Laboratoire actif dans la géométrie computationnelle")
    assert profile.description.short.fr.is_auto_generated is True
    assert profile.description.short.fr.is_auto_corrected is True
    assert profile.description.short.fr.is_auto_translated is True
    assert profile.description.short.fr.translated_from == "en"

    assert profile.external_key.en == "10434"
    assert profile.external_key.fr == "10434"
    assert profile.external_key.de == ""
    assert profile.external_url.en == "https://lcav.epfl.ch/"
    assert profile.external_url.fr == "https://lcav.epfl.ch/"
    assert profile.external_url.de == ""


def test_to_row_maps_page_profile_back_to_mysql_shape() -> None:
    key = make_key()
    profile = MySQLPageProfileMapper.from_row(ROW_FIXTURE, key=key)

    row = MySQLPageProfileMapper.to_row(profile)

    assert row["short_code"] == "LCAV"
    assert row["is_visible"] == 1

    assert row["numeric_id_en"] == "10434"
    assert row["numeric_id_fr"] == "10434"
    assert "numeric_id_de" not in row
    assert "numeric_id_it" not in row

    assert row["subtype_en"] == "Laboratory"
    assert row["subtype_fr"] == "Laboratoire"
    assert "subtype_de" not in row
    assert "subtype_it" not in row

    assert row["name_en_value"] == "Audiovisual Communications Laboratory"
    assert row["name_en_is_auto_generated"] == 0
    assert row["name_en_is_auto_corrected"] == 0
    assert row["name_en_is_auto_translated"] == 0
    assert "name_en_translated_from" not in row

    assert row["name_fr_value"] == "Laboratoire de communications audiovisuelles"
    assert row["name_fr_is_auto_generated"] == 0
    assert row["name_fr_is_auto_corrected"] == 0
    assert row["name_fr_is_auto_translated"] == 0
    assert row["name_fr_translated_from"] == "en"

    assert row["description_short_en_is_auto_generated"] == 1
    assert row["description_short_en_is_auto_corrected"] == 1
    assert row["description_short_en_is_auto_translated"] == 0
    assert row["description_short_en_value"].startswith("Laboratory active in computational geometry")

    assert row["description_short_fr_is_auto_generated"] == 1
    assert row["description_short_fr_is_auto_corrected"] == 1
    assert row["description_short_fr_is_auto_translated"] == 1
    assert row["description_short_fr_translated_from"] == "en"
    assert row["description_short_fr_value"].startswith("Laboratoire actif dans la géométrie computationnelle")

    assert row["external_key_en"] == "10434"
    assert row["external_key_fr"] == "10434"
    assert row["external_url_en"] == "https://lcav.epfl.ch/"
    assert row["external_url_fr"] == "https://lcav.epfl.ch/"

    assert "external_key_de" not in row
    assert "external_url_de" not in row


def test_from_row_with_none_returns_default_profile() -> None:
    key = make_key()

    profile = MySQLPageProfileMapper.from_row(None, key=key)

    assert profile.key == key
    assert profile.short_code == ""
    assert profile.is_visible is True
    assert profile.numeric_id.en == ""
    assert profile.name.en.value == ""
    assert profile.description.short.en.value == ""


def test_round_trip_preserves_meaningful_values() -> None:
    key = make_key()

    profile = MySQLPageProfileMapper.from_row(ROW_FIXTURE, key=key)
    row = MySQLPageProfileMapper.to_row(profile)

    assert row["short_code"] == ROW_FIXTURE["short_code"]
    assert row["is_visible"] == ROW_FIXTURE["is_visible"]
    assert row["name_en_value"] == ROW_FIXTURE["name_en_value"]
    assert row["name_fr_value"] == ROW_FIXTURE["name_fr_value"]
    assert row["name_fr_translated_from"] == ROW_FIXTURE["name_fr_translated_from"]
    assert row["description_long_en_value"] == ROW_FIXTURE["description_long_en_value"]
    assert row["description_long_fr_value"] == ROW_FIXTURE["description_long_fr_value"]
    assert row["external_url_en"] == ROW_FIXTURE["external_url_en"]
    assert row["external_url_fr"] == ROW_FIXTURE["external_url_fr"]
