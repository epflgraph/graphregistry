from __future__ import annotations
from typing import Any
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldKey, NodeFieldList, NodeKey, NodeList
from graphregistry.domain.models.mdl_pageprofile import PageProfile


class MySQLNodeRepository:
    def __init__(self, db=None, registry_db=None, glbcfg: GlobalConfig | None = None, engine_name: str = "xaas_coresrv") -> None:
        if db is None or registry_db is None:
            from graphregistry.core.dbbridge import RegistryDB, db as default_db

            db = db or default_db
            registry_db = registry_db or RegistryDB()

        self.db = db
        self.registry_db = registry_db
        self.glbcfg = glbcfg or GlobalConfig()
        self.engine_name = engine_name

    def _get_schema(self, object_type: str) -> str:
        return self.glbcfg.object_type_to_schema.get(object_type, self.glbcfg.schema_registry)

    def exists(self, key: NodeKey) -> bool:
        schema_name = self._get_schema(key.object_type)
        out = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT COUNT(*)
                FROM {schema_name}.Nodes_N_Object
                WHERE (institution_id, object_type, object_id)
                    = (:institution_id, :object_type, :object_id);
            """,
            params=key.model_dump(mode="python"),
        )
        return bool(isinstance(out, list) and len(out) > 0 and out[0][0] > 0)

    def exists_many(self, key_list: list[NodeKey]) -> list[bool]:
        return [self.exists(key) for key in key_list]

    # Method: Fetch node data and construct Node object
    def get(self, key: NodeKey) -> Node | None:

        # Check if node exists first (return None if not found)
        if not self.exists(key):
            return None

        # Get registry schema name based on object type
        schema_registry = self.glbcfg.object_type_to_schema[key.object_type]

        # Fetch basic node data (without custom fields)
        out = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT object_title, text_source, raw_text
                  FROM {schema_registry}.Nodes_N_Object
                 WHERE (institution_id, object_type, object_id)
                     = (:institution_id, :object_type, :object_id);
            """,
            params=key.model_dump(mode="python"),
        )
        if len(out) > 0:
            object_title, text_source, raw_text = out[0]
        else:
            return None

        # Fetch custom fields for the node
        custom_fields = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT field_language, field_name, field_value
                  FROM {schema_registry}.Data_N_Object_T_CustomFields
                 WHERE (institution_id, object_type, object_id)
                     = (:institution_id, :object_type, :object_id);
            """,
            params=key.model_dump(mode="python"),
        )
        custom_fields_dict = [
            {
                "field_language" : field_language,
                "field_name"     : field_name,
                "field_value"    : field_value
            }
            for field_language, field_name, field_value in custom_fields
        ]

        # Fetch page profile data for the node (if exists)
        out = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT numeric_id_en, numeric_id_fr, numeric_id_de, numeric_id_it, short_code, subtype_en, subtype_fr, subtype_de, subtype_it, name_en_is_auto_generated, name_en_is_auto_corrected, name_en_is_auto_translated, name_en_translated_from, name_en_value, name_fr_is_auto_generated, name_fr_is_auto_corrected, name_fr_is_auto_translated, name_fr_translated_from, name_fr_value, name_de_is_auto_generated, name_de_is_auto_corrected, name_de_is_auto_translated, name_de_translated_from, name_de_value, name_it_is_auto_generated, name_it_is_auto_corrected, name_it_is_auto_translated, name_it_translated_from, name_it_value, description_short_en_is_auto_generated, description_short_en_is_auto_corrected, description_short_en_is_auto_translated, description_short_en_translated_from, description_short_en_value, description_short_fr_is_auto_generated, description_short_fr_is_auto_corrected, description_short_fr_is_auto_translated, description_short_fr_translated_from, description_short_fr_value, description_short_de_is_auto_generated, description_short_de_is_auto_corrected, description_short_de_is_auto_translated, description_short_de_translated_from, description_short_de_value, description_short_it_is_auto_generated, description_short_it_is_auto_corrected, description_short_it_is_auto_translated, description_short_it_translated_from, description_short_it_value, description_medium_en_is_auto_generated, description_medium_en_is_auto_corrected, description_medium_en_is_auto_translated, description_medium_en_translated_from, description_medium_en_value, description_medium_fr_is_auto_generated, description_medium_fr_is_auto_corrected, description_medium_fr_is_auto_translated, description_medium_fr_translated_from, description_medium_fr_value, description_medium_de_is_auto_generated, description_medium_de_is_auto_corrected, description_medium_de_is_auto_translated, description_medium_de_translated_from, description_medium_de_value, description_medium_it_is_auto_generated, description_medium_it_is_auto_corrected, description_medium_it_is_auto_translated, description_medium_it_translated_from, description_medium_it_value, description_long_en_is_auto_generated, description_long_en_is_auto_corrected, description_long_en_is_auto_translated, description_long_en_translated_from, description_long_en_value, description_long_fr_is_auto_generated, description_long_fr_is_auto_corrected, description_long_fr_is_auto_translated, description_long_fr_translated_from, description_long_fr_value, description_long_de_is_auto_generated, description_long_de_is_auto_corrected, description_long_de_is_auto_translated, description_long_de_translated_from, description_long_de_value, description_long_it_is_auto_generated, description_long_it_is_auto_corrected, description_long_it_is_auto_translated, description_long_it_translated_from, description_long_it_value, external_key_en, external_key_fr, external_key_de, external_key_it, external_url_en, external_url_fr, external_url_de, external_url_it, is_visible
                  FROM {schema_registry}.Data_N_Object_T_PageProfile
                 WHERE (institution_id, object_type, object_id)
                     = (:institution_id, :object_type, :object_id);
            """,
            params=key.model_dump(mode="python"),
        )
        if len(out) > 0:
            page_profile_dict = dict(zip(self.glbcfg.page_profile_columns, out[0]))
        else:
            page_profile_dict = {}

        # Construct Node object from fetched data
        node = Node(
            key          = key,
            title        = object_title,
            text_source  = text_source,
            raw_text     = raw_text,
            field_list   = NodeFieldList.from_json(data=custom_fields_dict, key=key),
            page_profile = PageProfile.from_json(data=page_profile_dict, key=key),
        )

        # Return the constructed Node object
        return node

    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        out = [node for node in (self.get(key) for key in key_list) if node is not None]
        return NodeList(node_list=out)

    # Save (insert) node data to persistence
    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Any:

        # Get registry schema name based on object type
        schema_registry = self.glbcfg.schema_registry

        # Upsert basic node data (without custom fields)
        self.db.execute_upsert_row(
            engine_name       = self.engine_name,
            schema_name       = schema_registry,
            table_name        = 'Nodes_N_Object',
            key_column_names  = ['institution_id', 'object_type', 'object_id'],
            key_column_values = [node.key.institution_id, node.key.object_type, node.key.object_id],
            upd_column_names  = ['object_title', 'text_source', 'raw_text'],
            upd_column_values = [node.title, node.text_source, node.raw_text],
            actions           = actions
        )

        # Upsert custom fields for the node
        for field in node.field_list.field_list:
            field_key = field.key
            self.db.execute_upsert_row(
                engine_name       = self.engine_name,
                schema_name       = schema_registry,
                table_name        = 'Data_N_Object_T_CustomFields',
                key_column_names  = ['institution_id', 'object_type', 'object_id', 'field_language', 'field_name'],
                key_column_values = [field_key.key.institution_id, field_key.key.object_type, field_key.key.object_id, field_key.field_language, field_key.field_name],
                upd_column_names  = ['field_value'],
                upd_column_values = [field.field_value],
                actions           = actions
            )

        # Build simplified page profile dict for the node (only include fields that are present in the page profile JSON)
        page_profile_json = node.page_profile.to_simplified_dict()
        page_profile_json_keys, page_profile_json_values = zip(*page_profile_json.items()) if page_profile_json else ([], [])

        # Upsert page profile data for the node
        self.db.execute_upsert_row(
            engine_name       = self.engine_name,
            schema_name       = schema_registry,
            table_name        = 'Data_N_Object_T_PageProfile',
            key_column_names  = ['institution_id', 'object_type', 'object_id'],
            key_column_values = [node.key.institution_id, node.key.object_type, node.key.object_id],
            upd_column_names  = page_profile_json_keys,
            upd_column_values = page_profile_json_values,
            actions           = actions
        )

    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return [self.save(node, actions=actions) for node in node_list.node_list]

    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        if not self.exists(key):
            return False

        if "commit" in actions:
            self.registry_db.delete_nodes_by_ids(
                institution_id=key.institution_id,
                object_type=key.object_type,
                nodes_id=[key.object_id],
                engine_name=self.engine_name,
                actions=actions,
            )
        return True

    def delete_many(self, key_list: list[NodeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool]:
        return [self.delete(key, actions=actions) for key in key_list]
