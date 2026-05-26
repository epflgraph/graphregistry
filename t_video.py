from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
from graphregistry.domain.models.entities.mdl_text import MultilingualText
import rich

# Streamable MP4 video URL
file_url = 'https://raw.githubusercontent.com/epflgraph/graphregistry/master/scripts/init/sample_sets/MATH-132_Lecture_01.mp4'

# Initialize the gateway
gtw_video = GraphAIVideoGateway(debug=False)
gtw_voice = GraphAIVoiceGateway(debug=False)

#-------------------------------------------------------#

# Get video object
video = gtw_video.get_video(file_url=file_url)
rich.print(video)

# Ensure we got a valid video object before proceeding
assert video is not None, "Failed to get video object"

#-------------------------------------------------------#

# Extract audio from video and get audio token
voice = gtw_video.extract_audio(input=video)
rich.print(voice)

# Ensure we got a valid voice object before proceeding
assert voice is not None, "Failed to extract audio from video"

#-------------------------------------------------------#

# Extract slides from video and get slide list
slides = gtw_video.extract_slides(input=video)
rich.print(slides)

# Ensure we got a valid slide list before proceeding
assert slides is not None, "Failed to extract slides from video"

#-------------------------------------------------------#

# Transcribe audio from video and get transcription results
transcript = gtw_voice.transcribe_audio(input=voice)
rich.print(transcript)

# Ensure we got valid transcription results before proceeding
assert transcript is not None, "Failed to transcribe audio from video"

#-------------------------------------------------------#