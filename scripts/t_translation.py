from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
from graphregistry.domain.models.entities.mdl_text import MultilingualText
import rich

gtw = GraphAITextTranslationGateway(debug=False)

ml_text: MultilingualText = gtw.translate_multilingual(
    text = MultilingualText({
        'en': 'The speed of light is approximately 299,792 kilometers per second.'
    }),
    source_language = 'en',
    target_languages = ('en', 'fr', 'de', 'it'),
)
rich.print_json(data=ml_text.to_json())
