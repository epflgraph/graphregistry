# adapters/mysql/edge_repository.py
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.domain.models.edge import Edge, EdgeKey, EdgeList

class MySQLEdgeRepository:

    def __init__(self, db, glbcfg, engine_name: str = "xaas_coresrv"):
        self.db = db
        self.glbcfg = glbcfg
        self.engine_name = engine_name

    def _get_schema(self, key: EdgeKey) -> str:
        schema_from = self.glbcfg.object_type_to_schema.get(
            key.from_object_type, self.glbcfg.schema_registry
        )
        schema_to = self.glbcfg.object_type_to_schema.get(
            key.to_object_type, self.glbcfg.schema_registry
        )

        if schema_from == self.glbcfg.schema_lectures or schema_to == self.glbcfg.schema_lectures:
            return self.glbcfg.schema_lectures
        elif schema_from == schema_to:
            return schema_from
        else:
            return self.glbcfg.schema_registry

    # ✅ YOUR FUNCTION LIVES HERE
    def exists(self, key: EdgeKey) -> bool:
        schema = self._get_schema(key)

        out = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT COUNT(*)
                FROM {schema}.Edges_N_Object_N_Object_T_ChildToParent
                WHERE (from_institution_id, from_object_type, from_object_id,
                       to_institution_id, to_object_type, to_object_id, context)
                    = ("{key.from_institution_id}", "{key.from_object_type}", "{key.from_object_id}",
                       "{key.to_institution_id}", "{key.to_object_type}", "{key.to_object_id}", "{key.context}");
            """,
            query_id="WbT78q0i",
        )

        return isinstance(out, list) and out[0][0] > 0.5