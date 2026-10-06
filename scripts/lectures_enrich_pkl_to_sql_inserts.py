from __future__ import annotations
from graphregistry.adapters.gateways.graphai.gtw_translation import GraphAITextTranslationGateway
from graphregistry.domain.models.entities.mdl_text import MultilingualText
import pickle, glob, rich, json, os

# Initialise translation gateway
gw = GraphAITextTranslationGateway()

# Location of the save enrich results
input_folder  = 'data/lecture_refined_concepts'
output_folder = 'data/lecture_refinement_translations'

# List pickle files in folder
list_of_files = glob.glob(input_folder+'/*.pkl')

# Define function for translating field
def _translate_field(input_str):
    return gw.translate_multilingual(
        text = MultilingualText(item_map={"en": input_str}),
        source_language  = "en",
        target_languages = ("fr", "de", "it"),
    )

# Get number of lectures
N_lectures = len(list_of_files)

# Loop over all pickle files
for k, in_file_path in enumerate(list_of_files):

    # Generate output file name
    out_file_path = in_file_path.replace('refined_concepts', 'refinement_translations').replace('enrichment_result', 'refinement_translations').replace('.pkl', '.json')

    # Check if output file exsits. If so, do not re-process
    if os.path.exists(out_file_path):
        print(f"Skipping {out_file_path} (already exists)")
        continue
    else:
        print(f"Processing lecture {k}/{N_lectures} ...")

    # Get object from pickle
    with open(in_file_path, "rb") as f:
        enrich_result = pickle.load(f)

    # Initialise output structure
    out_struct = {
        'id' : None,
        'title'              : {'en' : None, 'fr' : None, 'de' : None, 'it' : None},
        'short_description'  : {'en' : None, 'fr' : None, 'de' : None, 'it' : None},
        'medium_description' : {'en' : None, 'fr' : None, 'de' : None, 'it' : None},
        'long_description'   : {'en' : None, 'fr' : None, 'de' : None, 'it' : None}
    }

    # Get lecture id
    out_struct['id'] = enrich_result.lecture_id

    # Get lecture details in English
    out_struct['title']['en'] = enrich_result.title
    out_struct['short_description' ]['en'] = enrich_result.short_description
    out_struct['medium_description']['en'] = enrich_result.medium_description
    out_struct['long_description'  ]['en'] = enrich_result.long_description

    # Generate translations in Graph AI
    title_translations              = _translate_field(out_struct['title']['en'])
    short_description_translations  = _translate_field(out_struct['short_description' ]['en'])
    medium_description_translations = _translate_field(out_struct['medium_description']['en'])
    long_description_translations   = _translate_field(out_struct['long_description'  ]['en'])

    # Assign translations to FR, DE, and IT
    for lang in ['fr', 'de', 'it']:
        out_struct['title'][lang]              = title_translations.item_map[lang]
        out_struct['short_description' ][lang] = short_description_translations.item_map[ lang].replace("Conf\u00e9rence", "S\u00e9ance de cours").replace("conf\u00e9rence", "s\u00e9ance de cours")
        out_struct['medium_description'][lang] = medium_description_translations.item_map[lang].replace("Conf\u00e9rence", "S\u00e9ance de cours").replace("conf\u00e9rence", "s\u00e9ance de cours")
        out_struct['long_description'  ][lang] = long_description_translations.item_map[  lang].replace("Conf\u00e9rence", "S\u00e9ance de cours").replace("conf\u00e9rence", "s\u00e9ance de cours")

    # Save to JSON file
    print(out_file_path)
    with open(out_file_path, 'w') as fd:
        fd.write(json.dumps(out_struct, indent=4))

    rich.print_json(data=out_struct)
    # break