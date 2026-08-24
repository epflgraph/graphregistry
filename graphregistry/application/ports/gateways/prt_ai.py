# graphregistry/application/ports/gateways/prt_ai.py
from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.gateways.prt_embedding import TextEmbeddingGateway
from graphregistry.application.ports.gateways.prt_image import ImageProcessingGateway
from graphregistry.application.ports.gateways.prt_lectureenrich import LectureEnrichmentGateway
from graphregistry.application.ports.gateways.prt_translation import TextTranslationGateway
from graphregistry.application.ports.gateways.prt_video import VideoProcessingGateway
from graphregistry.application.ports.gateways.prt_voice import VoiceProcessingGateway

AIGateways = {
    "concept_detection": ConceptDetectionGateway,
    "text_embedding": TextEmbeddingGateway,
    "image_processing": ImageProcessingGateway,
    "translation": TextTranslationGateway,
    "lecture_enrichment": LectureEnrichmentGateway,
    "video_processing": VideoProcessingGateway,
    "voice_processing": VoiceProcessingGateway,
}