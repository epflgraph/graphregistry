from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptGateway
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_node import Node
from graphregistry.workflows.factories.fct_node import NodeFactory
import rich

gtw = GraphAIConceptGateway(debug=True)
node_factory = NodeFactory(concept_gateway=gtw)

node: Node = node_factory.create(
    key = NodeKey(
        institution_id = 'EPFL',
        object_type    = 'Course',
        object_id      = 'MATH-101',
    ),
    title = "Introduction to Geometry",
    raw_text = """
        To draw a straight line from any point to any point. To produce a finite straight line
        continuously in a straight line. To describe a circle with any center and radius. That
        all right angles equal one another. That, if a straight line falling on two straight
        lines makes the interior angles on the same side less than two right angles, the two
        straight lines, if produced indefinitely, meet on that side on which are the angles less
        than the two right angles.""",
    detect_concepts = True,
)
rich.print_json(data=node.detected_concepts.to_json())
rich.print_json(data=node.to_json())
