from __future__ import annotations
from graphregistry.adapters.gateways.graphai.gtw_translation import GraphAITextTranslationGateway
from graphregistry.domain.models.entities.mdl_text import MultilingualText
from graphdb.models.sqlquery import print_sql
import pickle, glob, rich, json, os

# Initialise translation gateway
gw = GraphAITextTranslationGateway()

# Location of the save enrich results
input_folder  = 'data/lecture_refinement_translations'

# List pickle files in folder
list_of_files = glob.glob(input_folder+'/*.json')

# Get number of lectures
N_lectures = len(list_of_files)

# Loop over all pickle files
for k, in_file_path in enumerate(sorted(list_of_files)):

    # Get object from pickle
    with open(in_file_path, "r") as f:
        d = json.loads(f.read())

    out_file_path = f"/home/dockerhost/dev/graphregistry/data/lecture_refinement_transl_sql/{d['id']}.sql"

    # Check if output file exsits. If so, do not re-process
    if os.path.exists(out_file_path):
        print(f"Skipping {out_file_path} (already exists)")
        continue
    else:
        print(f"Processing lecture {k}/{N_lectures} ...")

    # rich.print_json(data=d)

    sql_var_assign = ""
    for lang in ['en', 'fr', 'de', 'it']:
        sql_var_assign += f"""
            name_{lang}_is_auto_generated = 1,
            name_{lang}_is_auto_corrected = 0,
            name_{lang}_is_auto_translated = {'0' if lang=='en' else '1'},
            name_{lang}_translated_from = {'NULL' if lang=='en' else '"en"'},
            name_{lang}_value = "{d['title'][lang]}",

            description_short_{lang}_is_auto_generated = 1,
            description_short_{lang}_is_auto_corrected = 0,
            description_short_{lang}_is_auto_translated = {'0' if lang=='en' else '1'},
            description_short_{lang}_translated_from = {'NULL' if lang=='en' else '"en"'},
            description_short_{lang}_value = "{d['short_description'][lang]}",

            description_medium_{lang}_is_auto_generated = 1,
            description_medium_{lang}_is_auto_corrected = 0,
            description_medium_{lang}_is_auto_translated = {'0' if lang=='en' else '1'},
            description_medium_{lang}_translated_from = {'NULL' if lang=='en' else '"en"'},
            description_medium_{lang}_value = "{d['medium_description'][lang]}",

            description_long_{lang}_is_auto_generated = 1,
            description_long_{lang}_is_auto_corrected = 0,
            description_long_{lang}_is_auto_translated = {'0' if lang=='en' else '1'},
            description_long_{lang}_translated_from = {'NULL' if lang=='en' else '"en"'},
            description_long_{lang}_value = "{d['long_description'][lang]}"{',' if lang!='it' else ''}
        """

    full_sql_query = f"""
        UPDATE Data_N_Object_T_PageProfile
        SET {sql_var_assign}
        WHERE object_type = 'Lecture'
        AND object_id = '{d['id']}';
    """

    with open(out_file_path, 'w') as fd:
        fd.write(full_sql_query)

    # break