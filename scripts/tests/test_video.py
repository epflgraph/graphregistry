#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.common.config import GlobalConfig
from graphai_client import client as graphai
from graphai_client.client_api.text import extract_concepts_from_text
import rich, json

# Streamable MP4 video URL
video_url = 'https://raw.githubusercontent.com/epflgraph/graphregistry/master/scripts/init/sample_sets/MATH-132_Lecture_01.mp4'

# Output file base path
output_file_base_path = 'scripts/init/sample_sets/MATH-132_Lecture_01'

# Initialize global config
glbcfg = GlobalConfig()

# GraphAI API config file
graph_api_json = glbcfg.settings['graphai']['client_config_file']

#=============#
# Main method #
#=============#
def main():

    # Login and get auth token
    login_info = graphai.login(graph_api_json)

    #-----------------------------------------#
    # Method 1: Execute all steps in one call #
    #-----------------------------------------#
    if True:

        # Execute all steps in one call (fetch, process slides, process audio)
        video_info = graphai.process_video(
            video_url      = video_url,
            graph_api_json = graph_api_json,
            ocr_model      = 'tesseract',
            destination_languages = ['en']
        )

        # Apply concept detection and append to slides
        for k in range(len(video_info['slides'])):
            video_info['slides'][k]['concepts'] = extract_concepts_from_text(video_info['slides'][k]['en'], login_info=login_info)

        # Save video info to JSON file
        with open(f'{output_file_base_path}_Method_1.json', 'w') as fp:
            json.dump(video_info, fp, indent=4)

    #------------------------------------#
    # Method 2: Execute steps separately #
    #------------------------------------#
    else:

        # Fetch video by URL
        video_token, video_size, streams = graphai.download_url(
            video_url  = video_url,
            login_info = login_info
        )

        # Apply slide detection and OCR
        slides_language, slides = graphai.process_slides(
            video_token = video_token,
            login_info  = login_info,
            ocr_model   = 'tesseract',
            destination_languages = ['en']
        )

        # Apply concept detection and append to slides
        for k in range(len(slides)):
            slides[k]['concepts'] = extract_concepts_from_text(slides[k]['en'], login_info=login_info)

        # Transcribe and translate audio
        audio_language, audio_fingerprint, segments = graphai.process_audio(
            video_token = video_token,
            login_info  = login_info,
            destination_languages = ['en']
        )

        # Construct final video info
        video_info = {
            'url'               : video_url,
            'video_size'        : video_size,
            'video_token'       : video_token,
            'slides'            : slides,
            'slides_language'   : slides_language,
            'subtitles'         : segments,
            'audio_language'    : audio_language,
            'audio_fingerprint' : audio_fingerprint,
            'streams'           : streams
        }

        # Save video info to JSON file
        with open(f'{output_file_base_path}_Method_2.json', 'w') as fp:
            json.dump(video_info, fp, indent=4)

    # Print video info
    rich.print_json(data=video_info)

#===============#
# Run as script #
#===============#
if __name__ == '__main__':
    import multiprocessing as mp
    mp.set_start_method('spawn', force=True)
    main()