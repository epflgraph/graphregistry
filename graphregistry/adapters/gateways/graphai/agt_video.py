# graphregistry/adapters/gateways/graphai/agt_video.py
from __future__ import annotations
from pathlib import Path
from requests import post
from graphregistry.domain.models.entities.mdl_lecture import Video, Voice, Slide, SlideList
from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
import rich

#==================================#
# GraphAI Gateway: Video endpoints #
#==================================#
class GraphAIVideoGateway(GraphAIBaseGateway):

    #----------------------------------------------------------------#
    # Gateway method: Download video from URL and create video token #
    #----------------------------------------------------------------#
    def get_video(
        self,
        file_url: str,
        *,
        playlist: bool = False,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 900,
        launch_only: bool = False,
    ) -> Video | str | None:
    #----------------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Make the request to the GraphAI endpoint to retrieve or create the video token
        task_result = self._call_async_endpoint(
            endpoint   = "/video/retrieve_url",
            payload    = {"url": file_url, "playlist": playlist, "force": force},
            login_info = login_info,
            max_processing_time_s = max_processing_time_s,
            max_tries  = max_tries,
            wait_for_result = not launch_only,
        )

        # If the task result is None, it means the request failed or the video could not be processed
        if task_result is None:
            return None

        # Non-blocking mode: return GraphAI task id immediately
        if launch_only:
            return str(task_result)
        # Extract token and stream metadata defensively: GraphAI can return a
        # successful task with missing/partial token_status payload.
        token = task_result.get("token")
        if token is None:
            return None
        token = str(token)

        token_status = task_result.get("token_status")
        streams = token_status.get("streams") if isinstance(token_status, dict) else None
        first_stream = streams[0] if isinstance(streams, list) and streams else None
        first_stream = first_stream if isinstance(first_stream, dict) else {}

        try:
            fingerprint = self.fingerprint(input=token)
        except Exception:
            fingerprint = None

        codec       = first_stream.get("codec_name")
        duration    = first_stream.get("duration")
        bit_rate    = first_stream.get("bit_rate")
        sample_rate = first_stream.get("sample_rate")
        resolution  = first_stream.get("resolution")

        # Create video object with available information
        video = Video(
            token       = token,
            file_url    = file_url,
            fingerprint = fingerprint,
            codec       = codec,
            duration    = duration,
            bit_rate    = bit_rate,
            sample_rate = sample_rate,
            resolution  = resolution,
        )

        # Return video object
        return video

    #---------------------------------------------------------------------------------#
    # Gateway method: Fingerprint video (generate unique identifier based on content) #
    #---------------------------------------------------------------------------------#
    def fingerprint(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 900,
        launch_only: bool = False,
    ) -> str | None:
    #---------------------------------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Resolve video token from explicit token param or from input object/string
        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if isinstance(input, Video) else input

        # Make the request to the GraphAI endpoint to calculate the fingerprint for the given video token
        task_result = self._call_async_endpoint(
            endpoint   = "/video/calculate_fingerprint",
            payload    = {"token": video_token, "force": force},
            login_info = login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries  = max_tries,
            wait_for_result = not launch_only,
        )

        # If the task result is None, it means the request failed or the fingerprint could not be calculated
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        # Get the fingerprint result from the task result
        result = task_result.get("result")

        # Return the fingerprint as a string if it exists, otherwise return None
        return str(result) if result is not None else None

    #-----------------------------------------------------------------#
    # Gateway method: Extract audio from video and create audio token #
    #-----------------------------------------------------------------#
    def extract_audio(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
        recalculate_cached: bool = False,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 300,
        launch_only: bool = False,
    ) -> Voice | str | None:
    #-----------------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Resolve video token from explicit token param or from input object/string
        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if isinstance(input, Video) else input

        # Make the request to the GraphAI endpoint to extract audio from the given video token and create an audio token
        task_result = self._call_async_endpoint(
            endpoint = "/video/extract_audio",
            payload  = {
                "token": video_token,
                "recalculate_cached": recalculate_cached,
                "force": force,
            },
            login_info = login_info,
            max_processing_time_s = max_processing_time_s,
            max_tries  = max_tries,
            wait_for_result = not launch_only,
        )

        # If the task result is None, it means the request failed or the audio could not be extracted
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        # Initialize the GraphAIVoiceGateway to use its fingerprint method for getting the audio fingerprint
        gtw_voice = GraphAIVoiceGateway()

        # Get audio parameters
        token       = task_result['token']
        fingerprint = gtw_voice.fingerprint(input=token)
        duration    = task_result['duration']

        # Create voice object with available information
        voice = Voice(
            token       = token,
            fingerprint = fingerprint,
            duration    = duration,
        )

        # Return voice object
        return voice

    #-------------------------------------------------------------------#
    # Gateway method: Extract slides from video and create slide tokens #
    #-------------------------------------------------------------------#
    def extract_slides(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
        recalculate_cached: bool = False,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 6000,
        hash_thresh: float = 0.95,
        multiplier: int = 5,
        default_threshold: float = 0.05,
        include_first: bool = True,
        include_last: bool = True,
        launch_only: bool = False,
    ) -> SlideList | str | None:
    #-------------------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Resolve video token from explicit token param or from input object/string
        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if isinstance(input, Video) else input

        # Make the request to the GraphAI endpoint to extract slides from the given video token and create slide tokens
        task_result = self._call_async_endpoint(
            endpoint = "/video/detect_slides",
            payload = {
                "token": video_token,
                "recalculate_cached": recalculate_cached,
                "force": force,
                "parameters": {
                    "hash_thresh": hash_thresh,
                    "multiplier": multiplier,
                    "default_threshold": default_threshold,
                    "include_first": include_first,
                    "include_last": include_last,
                },
            },
            login_info = login_info,
            max_processing_time_s = max_processing_time_s,
            max_tries = max_tries,
            wait_for_result = not launch_only,
        )

        # If the task result is None, it means the request failed or the slides could not be extracted
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        # Create empty slide list
        slide_list = SlideList()

        # Loop over all slide detections and count how many are missing (i.e. have an inactive token status)
        for slide_number in task_result['slide_tokens']:

            # Extact slide parameters
            token       = task_result['slide_tokens'][slide_number]['token']
            timestamp   = task_result['slide_tokens'][slide_number]['timestamp']
            fingerprint = None

            # Create slide object with available information
            slide_list.item_list += [Slide(
                token       = token,
                timestamp   = timestamp,
                fingerprint = fingerprint,
            )]

        # Return slide list object
        return slide_list

    #---------------------------------------------------------#
    # Gateway method: Download video file given a video token #
    #---------------------------------------------------------#
    def download_file(
        self,
        token: str,
        file_path: str | Path,
        *,
        max_tries: int = 5,
        timeout: int = 60,
    ) -> Path | None:
    #---------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Create output path object
        output_path = Path(file_path)

        # Make the request to the GraphAI endpoint to download the video file corresponding to the given video token and save it to the specified file path
        response = self._request(
            url          = "/video/get_file",
            login_info   = login_info,
            request_func = post,
            headers      = {"Content-Type": "application/json"},
            json         = {"token": token},
            max_tries    = max_tries,
            timeout      = timeout,
        )

        # If the response is None, it means the request failed and the file could not be downloaded
        if response is None:
            return None

        output_path.write_bytes(response.content)

        # Return the output path where the video file was saved
        return output_path
