from graphdb.core.graphdb import GraphDB
import rich, pickle, os, re

# Initialise database connector
db = GraphDB()

# SQL template for fecthing lectures of a course
sql_template = """
   SELECT c.from_object_id AS lecture_id, c.field_value AS sort_number_per_academic_year, p.name_en_value AS lecture_name
     FROM graph_lectures.Data_N_Object_N_Object_T_CustomFields c
LEFT JOIN graph_lectures.Data_N_Object_T_PageProfile p
       ON (c.from_object_type, c.from_object_id) = (p.object_type, p.object_id)
    WHERE (c.from_object_type, c.to_object_type) = ('Lecture', 'Course')
      AND c.field_name = 'sort_number_per_academic_year'
      AND c.to_object_id = '[[course_id]]';
"""

# Example course ID
course_id = 'PHYS-101(a)'

# Replace placeholder in SQL template with actual course ID
sql_query = sql_template.replace('[[course_id]]', course_id)

# Pickle file name
pickle_file_out = f'data/branches/genai_tables_and_adapters/sort_number_global/lecture_sort_number_per_academic_year_{course_id}.pkl'

# Check if the pickle file exist
if os.path.exists(pickle_file_out):
    # If it exists, load 'out' from it
    print('Loading from pickle file ...')
    with open(pickle_file_out, 'rb') as f:
        out = pickle.load(f)
else:
    # If not, execute query, fetch results, and save results to pickle file
    out = db.execute_query(engine_name='coresrv', query=sql_query)
    with open(pickle_file_out, 'wb') as f:
        pickle.dump(out, f)

# Parse and horizontally sort sort_number_per_academic_year
def parse_sort_numbers(sort_number_per_academic_year):

    # Remove line breaks from string
    sort_number_per_academic_year = sort_number_per_academic_year.replace('\n',' ').replace('\r',' ')

    # Extract key-value pairs from string
    m = re.findall(r'\"([^\"]*)\"\:\s(\d*)', sort_number_per_academic_year)

    # Convert academic year to integer and pad order number
    m = [
        (int(year.replace('n/a', '-9999').split('-')[1]), str(order).zfill(6))
        for year, order in m
    ]

    # Sort academic years within each lecture descending
    # HORIZONTAL SORT:
    # key1 DESC, key2 DESC, key3 DESC, ...
    m = sorted(
        m,
        key=lambda x: x[0],
        reverse=True
    )

    return m

# Build sort key from the horizontally sorted values
def make_sort_key(m):

    # Build tuple for:
    # key1 DESC, value1 ASC, key2 DESC, value2 ASC, ...
    return tuple(
        x
        for year, order in m
        for x in (-year, order)
    )

# FIRST:
# Parse and horizontally sort each lecture
rows = [
    (
        lecture_id,
        parse_sort_numbers(sort_number_per_academic_year),
        lecture_name
    )
    for lecture_id, sort_number_per_academic_year, lecture_name in out
]

# SECOND:
# Vertically sort all lectures by:
# key1 DESC, value1 ASC, key2 DESC, value2 ASC, ...
# lecture_name ASC as final tie-breaker
rows = sorted(
    rows,
    key=lambda row: (
        make_sort_key(row[1]),
        row[2] or ''
    )
)

# Loop over sorted results and print sort_number_per_academic_year
for lecture_id, m, lecture_name in rows:
    print(m, lecture_name)
