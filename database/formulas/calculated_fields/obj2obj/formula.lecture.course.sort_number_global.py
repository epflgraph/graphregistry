from graphdb.core.graphdb import GraphDB
import rich, pickle, os, re
from collections import Counter

# Initialise database connector
db = GraphDB()

# SQL template for fecthing lectures of a course
sql_template_1 = """
   SELECT c.from_object_id AS lecture_id, c.field_value AS sort_number_per_academic_year, p.name_en_value AS lecture_name
     FROM graph_lectures.Data_N_Object_N_Object_T_CustomFields c
LEFT JOIN graph_lectures.Data_N_Object_T_PageProfile p
       ON (c.from_object_type, c.from_object_id) = (p.object_type, p.object_id)
    WHERE (c.from_object_type, c.to_object_type, c.context) = ('Lecture', 'Course', 'part of')
      AND c.field_name = 'sort_number_per_academic_year'
      AND c.record_deleted = 0
      AND p.record_deleted = 0
      AND c.to_object_id = '[[course_id]]';
"""

# SQL template for inserting calculated fields into cache table
# (batch insert: [[values]] expands to one value tuple per lecture)
sql_template_2 = """
REPLACE INTO graph_cache.Data_N_Object_N_Object_T_CalculatedFields
             (from_object_type, from_object_id, to_object_type, to_object_id, context, field_language, field_name, field_value, to_process, deleted)
     VALUES [[values]];
"""

# Fetch list of course ids
sql_list_of_course_ids = """
   SELECT DISTINCT c.to_object_id
     FROM graph_lectures.Data_N_Object_N_Object_T_CustomFields c
LEFT JOIN graph_lectures.Data_N_Object_T_PageProfile p
       ON (c.from_object_type, c.from_object_id) = (p.object_type, p.object_id)
    WHERE (c.from_object_type, c.to_object_type, c.context) = ('Lecture', 'Course', 'part of')
      AND c.field_name = 'sort_number_per_academic_year'
"""
list_of_course_ids = sorted([o[0] for o in db.execute_query(engine_name='coresrv', query=sql_list_of_course_ids)])

# Loop over all course ids
for course_id in list_of_course_ids: # ['PHYS-101(a)', 'CS-290']:

    # Print course ID being processed
    print(f'\n\nProcessing course {course_id} ...\n\n')

    # Replace placeholder in SQL template with actual course ID
    sql_query = sql_template_1.replace('[[course_id]]', course_id)

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

    # Parse sort_number_per_academic_year
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

        return m

    # Parse all lectures first so that order-number cycling can be
    # detected across all lectures of the course
    rows = [
        (
            lecture_id,
            parse_sort_numbers(sort_number_per_academic_year),
            lecture_name
        )
        for lecture_id, sort_number_per_academic_year, lecture_name in out
    ]

    # Initialise the dictionary to store order numbers per academic year
    orders_per_year = {}

    # Go through all lectures, collect order numbers per academic year
    for lecture_id, m, lecture_name in rows:
        for year, order in m:
            orders_per_year.setdefault(year, []).append(order)

    # Detect academic years where order numbers are duplicated
    # across lectures. Duplicate order numbers indicate that the
    # order number has cycled/restarted and is therefore unreliable.
    unreliable_years = {
        year
        for year, orders in orders_per_year.items()
        if any(
            count > 1
            for count in Counter(orders).values()
        )
    }

    # Set all order numbers for unreliable academic years to zero
    rows = [
        (
            lecture_id,
            [
                (
                    year,
                    '000000' if year in unreliable_years else order
                )
                for year, order in m
            ],
            lecture_name
        )
        for lecture_id, m, lecture_name in rows
    ]

    # Horizontally sort sort_number_per_academic_year
    rows = [
        (
            lecture_id,

            # Sort academic years within each lecture descending
            # HORIZONTAL SORT:
            # key1 DESC, key2 DESC, key3 DESC, ...
            sorted(
                m,
                key=lambda x: x[0],
                reverse=True
            ),

            lecture_name
        )
        for lecture_id, m, lecture_name in rows
    ]

    # Build sort key from the horizontally sorted values
    def make_sort_key(m):

        # Build tuple for:
        # key1 DESC, value1 ASC, key2 DESC, value2 ASC, ...
        return tuple(
            x
            for year, order in m
            for x in (-year, order)
        )

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

    # Build lecture_id to global_index mapping
    lecture_id_to_global_index = {
        lecture_id: global_index
        for global_index, (lecture_id, m, lecture_name) in enumerate(rows, start=1)
    }

    # Loop over sorted results and print sort_number_per_academic_year
    for lecture_id, m, lecture_name in rows:
        print(
            lecture_id_to_global_index[lecture_id],
            lecture_id,
            m,
            lecture_name
        )

    # Print final lecture_id to global_index mapping
    # rich.print(lecture_id_to_global_index)

    # Build batch of value tuples for all lectures of the course
    # (edge direction matches the source edge: Lecture 'part of' Course)
    values = ',\n'.join(
        f"('Lecture', '{lecture_id}', 'Course', '{course_id}', 'part of', 'n/a', 'sort_number_global', {global_index}, 1, 0)"
        for lecture_id, global_index in lecture_id_to_global_index.items()
    )

    # Replace placeholder in SQL template with the batch of values
    sql_query = sql_template_2.replace('[[values]]', values)

    # Insert the batch of lecture-to-global_index mapping into the cache table
    db.execute_query_in_shell(engine_name='coresrv', query=sql_query)
