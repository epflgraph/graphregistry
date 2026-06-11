import os, sys, pickle, rich

# Get input lecture id
lecture_id = sys.argv[1] if len(sys.argv) > 1 else None

# 0_0c68iko0
# Load from pickle file (for testing)
with open(f"data/lecture_refined_concepts/enrichment_result_{lecture_id}.pkl", "rb") as f:
    result = pickle.load(f)
    
rich.print(result)