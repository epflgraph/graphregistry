#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from tabulate import tabulate
import sys, re, termios, tty, Levenshtein

# Print in colour
def print_colour(msg, colour='white', background='black', style='normal', display_method=False):
    colour_codes = {
        'black'  : 30,
        'red'    : 31,
        'green'  : 32,
        'yellow' : 33,
        'blue'   : 34,
        'purple' : 35,
        'magenta': 35,
        'cyan'   : 36,
        'white'  : 37
    }
    background_codes = {
        'black'  : 40,
        'red'    : 41,
        'green'  : 42,
        'yellow' : 43,
        'blue'   : 44,
        'purple' : 45,
        'magenta': 45,
        'cyan'   : 46,
        'white'  : 47
    }
    style_codes = {
        'normal'  : 0,
        'bold'    : 1,
        'underline': 4,
        'blink'   : 5,
        'reverse' : 7,
        'hidden'  : 8
    }
    
    if display_method:
        frame = inspect.currentframe().f_back
        method = frame.f_code.co_name

        # Attempt to get class name from 'self' or 'cls'
        class_name = None
        if 'self' in frame.f_locals:
            class_name = type(frame.f_locals['self']).__name__
        elif 'cls' in frame.f_locals:
            class_name = frame.f_locals['cls'].__name__

        if class_name:
            msg = f"{class_name}.{method}(): {msg}"
        else:
            msg = f"{method}(): {msg}"

    print(f"\033[{style_codes[style]};{colour_codes[colour]};{background_codes[background]}m{msg}\033[0m")

# Pretty-print dataframe
def print_dataframe(df, title):
    print('')
    print_colour(title, colour='white', background='black', style='bold')
    print(tabulate(df, headers=df.columns, tablefmt='fancy_grid', showindex=False))
    print('')

# Calculate the normalized Levenshtein distance between two strings
def normalized_levenshtein(str1, str2):

    # Calculate the raw Levenshtein distance
    distance = Levenshtein.distance(str1, str2)

    # Get the length of the longer string
    max_len = max(len(str1), len(str2))

    # If both strings are empty, consider them identical
    if max_len == 0:
        return 1.0

    # Normalize the distance to a value between 0 and 1
    normalized_distance = 1 - (distance / max_len)

    # Return the normalized distance
    return normalized_distance

# Get input key from keyboard
def get_keypress():
    """Capture a single keypress without needing to press Enter."""
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        key = sys.stdin.read(1)  # Read a single character
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return key

# # Define function for playing system sounds
# def play_system_sound(msg_type, sound_strength):
#     sound_file = f"{msg_type}_{sound_strength}.aiff"
#     subprocess.run([glbcfg.settings['sound']['player_bin'], f'{glbcfg.settings["sound"]["data_path"]}/{sound_file}'])

# Function to get the table type from the table name
def get_table_type_from_name(table_name):

    match_gen_from_to_edges    = re.findall(r"Edges_N_[^_]*_[^_]*_N_[^_]*_[^_]*_T_(GBC|AS)$", table_name)
    match_obj_to_obj_edges     = re.findall(r"Edges_N_[^_]*_N_(?!Concept)[^_]*_T_[^_]*$", table_name)
    match_obj_to_concept_edges = re.findall(r"Edges_N_[^_]*_N_Concept_T_[^_]*$", table_name)
    match_data_object          = re.findall(r"Data_N_Object_T_[^_]*(_COPY)?$", table_name)
    match_data_obj_to_obj      = re.findall(r"Data_N_Object_N_Object_T_[^_]*$", table_name)
    match_doc_index            = re.findall(r"Index_D_[^_]*(_COPY)?$", table_name)
    match_link_index           = re.findall(r"Index_D_[^_]*_L_[^_]*_T_[^_]*(_Search)?(_COPY)?$", table_name)
    match_stats_object         = re.findall(r"Stats_N_Object_T_[^_]*$", table_name)
    match_stats_obj_to_obj     = re.findall(r"Stats_N_Object_N_Object_T_[^_]*$", table_name)
    match_buildup_docs         = re.findall(r'^IndexBuildup_Fields_Docs_[^_]*', table_name)
    match_buildup_links        = re.findall(r'^IndexBuildup_Fields_Links_ParentChild_[^_]*_[^_]*', table_name)
    match_scores_matrix        = re.findall(r"Edges_N_Object_N_Object_T_ScoresMatrix_AS$", table_name)

    if match_gen_from_to_edges:
        return 'from_to_edges'
    elif match_obj_to_obj_edges:
        return 'object_to_object'
    elif match_obj_to_concept_edges:
        return 'object_to_concept'
    elif match_data_object:
        if 'PageProfile' in table_name:
            return 'doc_profile'
        else:
            return 'object'
    elif match_data_obj_to_obj:
        return 'object_to_object'
    elif match_doc_index:
        return 'doc_index'
    elif match_link_index:
        return 'link_index'
    elif match_stats_object:
        return 'object'
    elif match_stats_obj_to_obj:
        return 'object_to_object'
    elif match_buildup_docs:
        return 'doc_index'
    elif match_buildup_links:
        return 'link_index'
    elif match_scores_matrix:
        return 'object_to_object'
    else:
        return None
