# graphregistry/domain/interfaces/gateways/gtw_ai.py
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.application.gateways.gtw_embedding import TextEmbeddingGateway
from graphregistry.application.gateways.gtw_image import ImageProcessingGateway
from graphregistry.application.gateways.gtw_lectureenrich import LectureEnrichmentGateway
from graphregistry.application.gateways.gtw_translation import TextTranslationGateway
from graphregistry.application.gateways.gtw_video import VideoProcessingGateway
from graphregistry.application.gateways.gtw_voice import VoiceProcessingGateway

AIGateways = {
    "concept_detection": ConceptDetectionGateway,
    "text_embedding": TextEmbeddingGateway,
    "image_processing": ImageProcessingGateway,
    "translation": TextTranslationGateway,
    "lecture_enrichment": LectureEnrichmentGateway,
    "video_processing": VideoProcessingGateway,
    "voice_processing": VoiceProcessingGateway,
}