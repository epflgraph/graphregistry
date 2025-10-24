#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from tabulate import tabulate
import sys, termios, tty, Levenshtein

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
