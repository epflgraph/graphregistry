from graphregistry.application.core.cor_registry import GraphRegistry

gr = GraphRegistry()
idxdb = gr.IndexDB()

idxdb.delete_loose_ends(update_loose_ends=False, actions=('eval', 'commit'))