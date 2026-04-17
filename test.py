from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptGateway
from graphregistry.workflows.factories.fct_node import NodeFactory
import rich

gtw = GraphAIConceptGateway(debug=True)
node_factory = NodeFactory(concept_gateway=gtw)

node = node_factory.create(
    detect_concepts=True,
    key=NodeKey(institution_id="EPFL", object_type="Course", object_id="MATH-101"),
    title="Geometry",
    raw_text="To draw a straight line from any point to any point.\nTo produce a finite straight line continuously in a straight line.\nTo describe a circle with any center and radius.\nThat all right angles equal one another.\nThat, if a straight line falling on two straight lines makes the interior angles on the same side less than two right angles, the two straight lines, if produced indefinitely, meet on that side on which are the angles less than the two right angles.",
)

node.detected_concepts.print_json()