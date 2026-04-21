from graphregistry.domain.models.entities.mdl_text import MultilingualText
from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
import rich


gtw = GraphAITextTranslationGateway(debug=True)

text = MultilingualText({"en": "The angle sum of a triangle is 180 degrees."})
translated = gtw.translate_multilingual(
    text=text,
    source_language="en",
    target_languages=("en", "fr", "de", "it"),
)

rich.print_json(data=translated.to_json())
