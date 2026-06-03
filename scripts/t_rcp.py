
from graphregistry.adapters.clients.rcp_models import send_llm_request
from datetime import datetime
# Start time (now())
start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


reply = send_llm_request(
    timeout=300.0,
    messages=[
        {
            "role": "user",
            "content": """

The folloing OCR content has been extracted from a sequence of keyframes belonging to one course lecture. I"m going to give you the following JSON data:
{
    "lecture_id" : "lecture identifier",
    "keyframes" : [
        "id_01" : {
            "ocr_content" : "long string",
            "concepts" : ["concept_01", "concept_02", ...]
        },
        "id_02" : {
            "ocr_content" : "long string",
            "concepts" : ["concept_01", "concept_02", ...]
        },
        ...
    ]
}
Wherein:
- "ocr_content" contains the OCR extraction using Google Cloud Vision API;
- "concepts" contains a list of concept names, extracted from the OCR with a concept detection algorithm, wherein a concept name corresponds exactly to the title of one Wikipedia page, in an ontology derived from Wikipedia pages.

Your task is two-fold:

1. Analyse the entire set of OCR extractions as a whole and generate a Title and Description of the lecture, inferred from the extracted content. Furthermore, you are to create 3 types of description: (a) a long paragraph-sized description; (b) a medium-sized description of 3 lines; (c) a short description in the format "Lecture about top_subject_1, top_subject_2, top_subject_3", wherein top_subject_X is not necessarily a Wikipedia page name, but a general category you think is suitable.

2. Also using the entire set of OCR extractions, return a list of 10-20 top concepts addressed in the lecture, wherein, preferrably, the spelling of the concept names correspond exactly to the title of existing Wikipedia pages.

3. Analyse each keyframe OCR individually, and, for each keyframe, compare the OCR content and the detected concepts, and remove the concepts that have been extracted due to too much fuzzyness, keeping only the ones you're sure are being mentioned in the keyframe OCR. You should also add missing concepts that have not been detected.

The output format should be:
{
    "lecture_id" : "lecture identifier (the same as input)",
    "title" : "your suggested lecture title",
    "long_description" : "long paragraph-sized description",
    "medium_description" : "medium-sized description of 3 lines",
    "short_description" : "Lecture about top_subject_1, top_subject_2, top_subject_3",
    "top_concepts" : ["top_concept_01", "top_concept_02", ...]
    "keyframes" : [
        "id_01" : {
            "refined_concepts" : ["refined_concept_01", "refined_concept_02", ...]
        },
        "id_02" : {
            "refined_concepts" : ["refined_concept_01", "refined_concept_02", ...]
        },
        ...
    ]
}

Notes:
- Some slides are heavily poluted because of OCR noise. If words are likely to be noise, like "powerpoint", "acrobat", "zoom", or "gmail", you should be able to detect and remove them.
- try and keep the title less than 60 characters
- no line breaks in any of the descriptions

Here's your data.


{
  "lecture_id": "0_192jngsv",
  "keyframes": [
    {
      "0_zxdz7z5f-0000": {
        "ocr_content": " Lists. Okay, HUMAN. . HUH? . N. BEFORE YOU. HIT COMPILE. YOU KNOW WHEN YOU'RE. FALLING ASLEEP, AND. YOU IMAGINE YOURSELF. WALKING OR. SOMETHING,. AND SUDDENLY YOU. MISSTEP, STUMBLE. AND JOLT AWAKE? . YEAH! . WELL, THAT'S WHAT A. SEGFAULT FEELS LIKE. . N. DOUBLE-CHECK YOUR. DAMN POINTERS, OKAY? . ICC-C Course 11. LISTEN UP. EPFL",
        "concepts": [
          "Human rights",
          "Sleep",
          "Walking",
          "Human body",
          "Human"
        ]
      }
    },
    {
      "0_zxdz7z5f-0001": {
        "ocr_content": " Reminder struct. struct data is a compound type. data_t is a synonym for struct_data. Also a guy. data is a variable of type struct_data. typedef struct _data. whole int;. real float;. data_t;. data_t data  5, 3.14;. data. data.integer  5. data.real  3.14. EPFL",
        "concepts": [
          "Function of a real variable",
          "Semiregular variable star",
          "Integer",
          "Integer (computer science)",
          "Integer factorization",
          "Data",
          "Real number",
          "Integer overflow",
          "Quadratic integer",
          "C data types",
          "Recursive data type",
          "Variable (computer science)",
          "Linked data structure",
          "Data structure alignment",
          "Abstract data type",
          "Eisenstein integer",
          "Single-precision floating-point format",
          "English compound",
          "Typedef",
          "Big data",
          "Data analysis",
          "Square-free integer",
          "Composite data type",
          "Data science",
          "Variable (mathematics)",
          "Enumerated type",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "Double-precision floating-point format",
          "C++11",
          "Variable star",
          "Persistent data structure",
          "Data management",
          "Data warehouse",
          "Data model",
          "Data set",
          "Data structure",
          "Compound (linguistics)",
          "Data type"
        ]
      }
    },
    {
      "0_zxdz7z5f-0002": {
        "ocr_content": " Reminder struct. EPFL",
        "concepts": [
          "École Polytechnique Fédérale de Lausanne"
        ]
      }
    },
    {
      "0_zxdz7z5f-0003": {
        "ocr_content": " Reminder struct. struct _tuple is also a compound type. Both of its members have compound types. tuple is a variable of type struct_tuple. Tuple. tuple.left. typedef struct _tuple. data_t left;. data_t right;. tuple_t;. tuple_t tuple . };. 5, 3.14, -5, -3.14. Tuple. right. tuple.left.integer  5. tuple.right.integer  -5. Tuple. left. Real  3.14. tuple.right. real  -3.14. EPFL",
        "concepts": [
          "Superkey",
          "Function of a real variable",
          "Tuple",
          "Semiregular variable star",
          "Integer",
          "Integer (computer science)",
          "Real coordinate space",
          "Product type",
          "Integer factorization",
          "Relational algebra",
          "Tuple relational calculus",
          "Far-left politics",
          "Left-wing politics",
          "Template metaprogramming",
          "Real number",
          "Integer overflow",
          "Quadratic integer",
          "Recursive data type",
          "Variable (computer science)",
          "Abstract data type",
          "Eisenstein integer",
          "English compound",
          "Left-libertarianism",
          "Relational database",
          "Relational model",
          "Square-free integer",
          "Tree (data structure)",
          "Composite data type",
          "Unit type",
          "Variable (mathematics)",
          "French Left",
          "Left communism",
          "Centre-left politics",
          "New Left",
          "Enumerated type",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "C++11",
          "Variable star",
          "Compound (linguistics)",
          "Data type"
        ]
      }
    },
    {
      "0_zxdz7z5f-0004": {
        "ocr_content": " PTT. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. // date. // the duration of the call. call_record_t;. EPFL",
        "concepts": [
          "Customer experience",
          "Multiple dispatch",
          "Operator overloading",
          "Abstract data type",
          "Customer satisfaction",
          "C syntax",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0005": {
        "ocr_content": " PTT. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call_record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "C syntax",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0006": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int *total_minutes). typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. {. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0007": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int *total_minutes). typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. {. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0008": {
        "ocr_content": " PTT. void time_total(.. const data_t *pdata,. int *total_minutes). for (int i  0; i  10; i++). total_minutes [i]  0;. call record_t* current. pdata->records;. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0009": {
        "ocr_content": " PTT. void time_total(.. const data_t *pdata,. int total minutes). for (int i  0; i  10; i++). total_minutes [i]  0;. call record_t* current. pdata->records;. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call_record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0010": {
        "ocr_content": " PTT. void time_total(.. const data_t *pdata,. int total_minutes). for (int i  0; i  10; i++). total_minutes [i]  0;. call record_t* current. pdata->records;. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0011": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int total minutes). {. for (int i  0; i  10; i++). total_minutes [i]  0;. call record_t* current. pdata->records;. while (. current pdata-> records + pdata->M). typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. {. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0012": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int total_minutes). {. for (int i  0; i  10; i++). total_minutes[i]  0;. call record_t* current. pdata->records;. while (. current pdata-> records + pdata->M). typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. {. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management"
        ]
      }
    },
    {
      "0_zxdz7z5f-0013": {
        "ocr_content": " PTT. void time_total(.. const data_t *pdata,. int total minutes). for (int i  0; i  10; i++). total_minutes[i]  0;. call record_t* current. pdata->records;. while (. current pdata->records + pdata->M). {. current->minutes;. total_minutes [current->no_client]. current++;. }. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. {. int tel;. tariff_t;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. typedef struct data. {. int M, N;. call record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "Alternating current",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Direct current",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Electric current",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management",
          "Current source"
        ]
      }
    },
    {
      "0_zxdz7z5f-0014": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int total_minutes). {. for (int i  0; i  10; i++). total_minutes[i]  0;. call record_t* current. pdata->records;. while (. current pdata->records + pdata->M). {. current->minutes;. total_minutes [current->no_client]. current++;. }. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. {. int tel;. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. tariff_t;. typedef struct data. {. int M, N;. call_record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "Alternating current",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Direct current",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Electric current",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management",
          "Current source"
        ]
      }
    },
    {
      "0_zxdz7z5f-0015": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int total minutes). {. for (int i  0; i  10; i++). total_minutes[i]  0;. call record_t* current. pdata->records;. while (. current pdata->records + pdata->M). {. current->minutes;. total_minutes [current->no_client]. current++;. }. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. {. int tel;. tariff_t;. {. typedef struct data. int M, N;. call_record_t *records;. price_t *rates;. data_t;. EPFL. Fo. 1",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "Alternating current",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Direct current",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Electric current",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management",
          "Current source"
        ]
      }
    },
    {
      "0_zxdz7z5f-0016": {
        "ocr_content": " PTT. void time_total(. const data_t *pdata,. int total_minutes). {. for (int i  0; i  10; i++). total_minutes [i]  0;. call record_t* current. pdata->records;. while (. current pdata->records + pdata->M). {. int tel;. tariff_t;. {. total_minutes [current->no_client]. current->minutes;. current++;. }. typedef struct call_record. int no_client;. // customer number. int no_tel_call; // tel number called. int date;. int minutes;. call_record_t;. typedef struct tariff. // date. // the duration of the call. // phone number. float chf_per_minute; // rate. typedef struct data. {. int M, N;. call_record_t *records;. price_t *rates;. data_t;. EPFL",
        "concepts": [
          "Generic top-level domain",
          "Emergency telephone number",
          "Customer experience",
          "Telephone number",
          "Multiple dispatch",
          "Mobile phone",
          "Void type",
          "Prank call",
          "Linked data structure",
          "Operator overloading",
          "Abstract data type",
          "Typedef",
          "Composite data type",
          "Customer satisfaction",
          "Telephone call",
          "T-carrier",
          "Alternating current",
          "C syntax",
          "Pointer (computer programming)",
          ".int",
          "Enumerated type",
          "Direct current",
          "Customer lifetime value",
          "Record (computer science)",
          "C++11",
          "Electric current",
          "Persistent data structure",
          "ALGOL 68",
          "Customer relationship management",
          "Current source"
        ]
      }
    },
    {
      "0_zxdz7z5f-0017": {
        "ocr_content": " Erratum. EPFL",
        "concepts": [
          "École Polytechnique Fédérale de Lausanne"
        ]
      }
    },
    {
      "0_zxdz7z5f-0018": {
        "ocr_content": " Erratum. typedef struct chem_element. the symbol[3]; short at_no;. float at mass;. chem_element_t;. EPFL. 1",
        "concepts": [
          "Conservation of mass",
          "Mass",
          "Atomic mass",
          "Molecular mass",
          "Cobalt",
          "Mass spectrometry",
          "Mass in special relativity",
          "Chemical symbol",
          "Earth mass"
        ]
      }
    },
    {
      "0_zxdz7z5f-0019": {
        "ocr_content": " Erratum. Its size is not really 9 (3+2+4). printf(\"chem_element_t: %ldn\",. sizeof(chem_element_t));. // Display 12. The compilers (often) align it. memory to multiples of 4. Padding... You should always rely on sizeof. typedef struct chem_element. the symbol[3]; short at_no;. float at mass;. chem_element_t;. EPFL",
        "concepts": [
          "Semiconductor memory",
          "Molecular mass",
          "Bootstrapping (compilers)",
          "Cobalt",
          "Mass spectrometry",
          "Cross compiler",
          "Memory",
          "Optimizing compiler",
          "Semantic memory",
          "Self-hosting (compilers)",
          "Compiler",
          "Chemical symbol",
          "Memory management",
          "Computer memory",
          "Compiler-compiler"
        ]
      }
    },
    {
      "0_zxdz7z5f-0020": {
        "ocr_content": " Static tables. EPFL",
        "concepts": [
          "Life table",
          "Template metaprogramming",
          "Lookup table",
          "Routing table"
        ]
      }
    },
    {
      "0_zxdz7z5f-0021": {
        "ocr_content": " Static tables. For a type T we can define an array of N elements of type T. EPFL",
        "concepts": [
          "Dynamic array",
          "Dependent type",
          "Type system",
          "Array (data structure)",
          "Array (data type)",
          "Type inference",
          "Antenna array",
          "Intuitionistic type theory",
          "Lookup table",
          "Type theory",
          "Phased array",
          "Type conversion",
          "Type safety",
          "Array programming",
          "Data type"
        ]
      }
    },
    {
      "0_zxdz7z5f-0022": {
        "ocr_content": " Static tables. For a type T we can define an array of N elements of type T. T tableau_static [10]; // variable of type T[10]. N is a constant. Sometimes a variable is allowed by some compilers. + Easy to declare and use. - Lifetime limited to block/function. - Fixed size - you can't change it. LILA. EPFL",
        "concepts": [
          "Nested function",
          "Semiregular variable star",
          "Dynamic array",
          "Dependent type",
          "Type system",
          "Array (data structure)",
          "Declarative programming",
          "Basic block",
          "Variable (computer science)",
          "Array (data type)",
          "Bootstrapping (compilers)",
          "Type inference",
          "Cross compiler",
          "Antenna array",
          "Intuitionistic type theory",
          "Lookup table",
          "Block (programming)",
          "One-way compression function",
          "Type theory",
          "Optimizing compiler",
          "Phased array",
          "Block cipher",
          "Type conversion",
          "Self-hosting (compilers)",
          "Compiler",
          "Type safety",
          "Variable star",
          "Compiler-compiler",
          "Anonymous function",
          "Array programming",
          "Data type"
        ]
      }
    },
    {
      "0_zxdz7z5f-0023": {
        "ocr_content": " Static tables. For a type T we can define an array of N elements of type T. T tableau_static [10]; // variable of type T[10]. N is a constant. Sometimes a variable is allowed by some compilers. + Easy to declare and use. Lifetime limited to block/function. Fixed size - it cannot be changed. LINA. EPFL",
        "concepts": [
          "Nested function",
          "Semiregular variable star",
          "Dynamic array",
          "Dependent type",
          "Type system",
          "Array (data structure)",
          "Declarative programming",
          "Basic block",
          "Variable (computer science)",
          "Array (data type)",
          "Bootstrapping (compilers)",
          "Type inference",
          "Cross compiler",
          "Antenna array",
          "Intuitionistic type theory",
          "Lookup table",
          "Block (programming)",
          "One-way compression function",
          "Type theory",
          "Optimizing compiler",
          "Phased array",
          "Block cipher",
          "Type conversion",
          "Self-hosting (compilers)",
          "Compiler",
          "Type safety",
          "Variable star",
          "Compiler-compiler",
          "Anonymous function",
          "Array programming",
          "Data type"
        ]
      }
    },
    {
      "0_zxdz7z5f-0024": {
        "ocr_content": " Dynamic tables. EPFL",
        "concepts": [
          "Dynamic loading",
          "Hash table",
          "Dynamic array",
          "Dynamic linker"
        ]
      }
    },
    {
      "0_zxdz7z5f-0025": {
        "ocr_content": " Dynamic tables. 0. For a type T we can allocate an array of size N of type T*. T *dynamic table  malloc(N * sizeof(T)); // variable of type T*. EPFL. 1",
        "concepts": [
          "Dynamic loading",
          "Hash table",
          "Semiregular variable star",
          "Dynamic array",
          "Memory management unit",
          "Memory pool",
          "Linked list",
          "Dependent type",
          "Type system",
          "Array (data structure)",
          "Variable (computer science)",
          "Array (data type)",
          "Type inference",
          "Antenna array",
          "Intuitionistic type theory",
          "Dynamic linker",
          "Type theory",
          "Phased array",
          "Sizeof",
          "C syntax",
          "Pointer (computer programming)",
          "Type conversion",
          "C (programming language)",
          "Type safety",
          "Variable star",
          "Memory management",
          "C++",
          "Array programming",
          "Virtual method table",
          "Data type"
        ]
      }
    },
    {
      "0_zxdz7z5f-0026": {
        "ocr_content": " Flexible size storage. Specifications. EPFL. FF",
        "concepts": [
          "Specification (technical standard)",
          "Grid energy storage",
          "Energy storage",
          "Pumped-storage hydroelectricity",
          "Object storage",
          "Network-attached storage",
          "Hydrogen storage",
          "Functional specification",
          "Software requirements specification"
        ]
      }
    },
    {
      "0_zxdz7z5f-0027": {
        "ocr_content": " Flexible size storage. Specifications. Can we define a data structure of variable size? . •. We have elements of a certain type T that we want to store. EPFL",
        "concepts": [
          "Purely functional data structure",
          "Specification (technical standard)",
          "Grid energy storage",
          "Dependent type",
          "Type system",
          "Variable (computer science)",
          "Energy storage",
          "Pumped-storage hydroelectricity",
          "Type inference",
          "Intuitionistic type theory",
          "Type theory",
          "Object storage",
          "Dependent and independent variables",
          "Network-attached storage",
          "Type conversion",
          "Type safety",
          "Persistent data structure",
          "Categorical variable",
          "Data structure",
          "Functional specification",
          "Data type",
          "Software requirements specification"
        ]
      }
    },
    {
      "0_zxdz7z5f-0028": {
        "ocr_content": " The idea. EPFL",
        "concepts": [
          "Idea",
          "École Polytechnique Fédérale de Lausanne"
        ]
      }
    },
    {
      "0_zxdz7z5f-0029": {
        "ocr_content": " A cell of integers. Implementation. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Square-free integer",
          "Cell (biology)",
          "Ring of integers",
          "Gaussian integer",
          "Cell adhesion",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0030": {
        "ocr_content": " A cell of integers. Implementation. typedef struct _cell. 23. next. -13. next. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Square-free integer",
          "Cell (biology)",
          "Ring of integers",
          "Gaussian integer",
          "Cell adhesion",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0031": {
        "ocr_content": " A cell of integers. Implementation. typedef struct _cell. }. int content;. ??? next;. cell_t;. 23. next. -13. next. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "T cell",
          "B cell",
          "T helper cell",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Square-free integer",
          "Cell (biology)",
          "Ring of integers",
          "Gaussian integer",
          "Cell adhesion",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Naive T cell",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0032": {
        "ocr_content": " A cell of integers. Implementation. typedef struct _cell. Instead of?? ? We should put... {. int content;. ??? next;. cell_t;. 23. next. -13. next. Option 1. Option 2. struct _cell. struct cell*. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Bond option",
          "T cell",
          "Put option",
          "B cell",
          "T helper cell",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Option style",
          "Stem cell",
          "Type signature",
          "Rust (programming language)",
          "Square-free integer",
          "Implementation",
          "Cell (biology)",
          "Asian option",
          "Exotic option",
          "Pointer (computer programming)",
          "Ring of integers",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "Binary option",
          "Computer program",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Options strategy",
          "Naive T cell",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0033": {
        "ocr_content": " A cell of integers. Implementation. Instead of?? ? We should put... typedef struct _cell. {. int content;. ??? next;. cell_t;. 23. next. -13. next. Option 1. Option 2. struct _cell. struct cell*. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Bond option",
          "T cell",
          "Put option",
          "B cell",
          "T helper cell",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Option style",
          "Stem cell",
          "Type signature",
          "Rust (programming language)",
          "Square-free integer",
          "Implementation",
          "Cell (biology)",
          "Asian option",
          "Exotic option",
          "Pointer (computer programming)",
          "Ring of integers",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "Binary option",
          "Computer program",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Options strategy",
          "Naive T cell",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0034": {
        "ocr_content": " A cell of integers. Option 1. typedef struct_cell. {. Option 1. int content;. struct cell next;. cell_t;. struct _cell. test.c:45:18: error: field has incomplete type'struct _cell'. struct cell next;. we want to use a guy who is not yet. completely defined. https://javier.xyz/droste-creator. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Bond option",
          "T cell",
          "Put option",
          "B cell",
          "T helper cell",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Option style",
          "Stem cell",
          "Type signature",
          "Rust (programming language)",
          "Square-free integer",
          "Cell (biology)",
          "Asian option",
          "Exotic option",
          "C syntax",
          "Pointer (computer programming)",
          "Ring of integers",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "Binary option",
          "Mantoux test",
          "Computer program",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Naive T cell",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0035": {
        "ocr_content": " A cell of integers. Option 1. typedef struct_cell. {. Option 1. int content;. struct cell next;. cell_t;. struct _cell. cell. content. next. test.c:45:18: error: field has incomplete type'struct _cell'. struct cell next;. we want to use a guy who is not yet. completely defined. https://javier.xyz/droste-creator. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Bond option",
          "T cell",
          "Put option",
          "B cell",
          "T helper cell",
          "Real options valuation",
          "Quadratic integer",
          "Content management",
          "Eisenstein integer",
          "Option style",
          "Stem cell",
          "Type signature",
          "Web content",
          "Rust (programming language)",
          "Square-free integer",
          "Cell (biology)",
          "Asian option",
          "Exotic option",
          "C syntax",
          "Pointer (computer programming)",
          "Ring of integers",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "Binary option",
          "Mantoux test",
          "Computer program",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Content management system",
          "Naive T cell",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0036": {
        "ocr_content": " A cell of integers. Option 3. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Square-free integer",
          "Cell (biology)",
          "Ring of integers",
          "Gaussian integer",
          "Binary option",
          "Cell adhesion",
          "Cell biology",
          "Coprime integers",
          "Employee stock option",
          "Cell cycle",
          "Options strategy",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0037": {
        "ocr_content": " A cell of integers. Option 3. typedef struct_cell. {. int content;. cell_t* next;. cell_t;. shallow.c:45:5: error: unknown type name 'cell_t'. cell_t *next;. The type cell_t is unknown - it is a synonym. Which is being defined... Option 3. cell_t*. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "T cell",
          "B cell",
          "T helper cell",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Square-free integer",
          "Cell (biology)",
          "Ring of integers",
          "Gaussian integer",
          "Binary option",
          "Cell biology",
          "Coprime integers",
          "Employee stock option",
          "Cell cycle",
          "Options strategy",
          "Naive T cell",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0038": {
        "ocr_content": " A cell of integers. Option 2. 23. next. -13. next. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Bond option",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Square-free integer",
          "Cell (biology)",
          "Ring of integers",
          "Gaussian integer",
          "Binary option",
          "Cell adhesion",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Options strategy",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0039": {
        "ocr_content": " A cell of integers. Option 2. typedef struct _cell. int content;. struct _cell next;. cell_t;. Instead of?? ? We should put... Option 2. struct cell. _cell_t. 23. next. _cell_t. -13. j--. next. EPFL",
        "concepts": [
          "Cell culture",
          "Integer",
          "Integer factorization",
          "Bond option",
          "T cell",
          "Put option",
          "B cell",
          "T helper cell",
          "Real options valuation",
          "Quadratic integer",
          "Eisenstein integer",
          "Stem cell",
          "Type signature",
          "Rust (programming language)",
          "Square-free integer",
          "Cell (biology)",
          "Pointer (computer programming)",
          "Ring of integers",
          "Gaussian integer",
          "Union type",
          "Record (computer science)",
          "Binary option",
          "Computer program",
          "Cell biology",
          "Coprime integers",
          "Cell cycle",
          "Options strategy",
          "Naive T cell",
          "Option (finance)",
          "Algebraic integer"
        ]
      }
    },
    {
      "0_zxdz7z5f-0040": {
        "ocr_content": " What is X? . NULL. Often when a pointer is not used, it is used. affects the value NULL. typedef struct_cell. int content;. struct _cell next;. cell_t;. •. Like \"zero\" but for pointers. °. We can test if a pointer is equal to NULL. EPFL",
        "concepts": [
          "Null pointer",
          "Affect display",
          "T cell",
          "T helper cell",
          "Type signature",
          "Rust (programming language)",
          "Smart pointer",
          "Affect (psychology)",
          "Tagged pointer",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Function pointer",
          "Mantoux test",
          "Computer program"
        ]
      }
    },
    {
      "0_zxdz7z5f-0041": {
        "ocr_content": " What is X? . NULL. Often when a pointer is not used, it is used. affects the value NULL. •. Like \"zero\" but for pointers. °. We can test if a pointer is equal to NULL. typedef struct_cell. int content;. struct cell* next;. cell_t;. EPFL",
        "concepts": [
          "Null pointer",
          "Affect display",
          "T cell",
          "T helper cell",
          "Type signature",
          "Rust (programming language)",
          "Smart pointer",
          "Affect (psychology)",
          "Tagged pointer",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Function pointer",
          "Mantoux test",
          "Computer program"
        ]
      }
    },
    {
      "0_zxdz7z5f-0042": {
        "ocr_content": " Example. Static. typedef struct_cell. typedef. -first-. -second. . {. ?. next. ?. next. int content;. struct cell* next;. cell_t;. cell_t first, second;. first.content  23;. EPFL",
        "concepts": [
          "T cell",
          "T helper cell",
          "Typedef",
          "Type signature",
          "Rust (programming language)",
          "Composite data type",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Function pointer",
          "Computer program",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0043": {
        "ocr_content": " Example. Static. typedef struct _cell. {. int content;. struct _cell next;. cell_t;. cell_t first, second;. first.content  23;. first.next  &second;. second.content  -13;. second.next  NULL;. cell_t *head  &first;. -first-. 23. 23. next. second. . -13. next. EPFL",
        "concepts": [
          "Null pointer",
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "T helper cell",
          "Immunotherapy",
          "Merkel-cell carcinoma",
          "Type signature",
          "Rust (programming language)",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0044": {
        "ocr_content": " Example. Static. typedef struct _cell. typedef. {. int content;. struct _cell next;. cell_t;. cell_t first, second;. first.content  23;. first.next  &second;. second.content  -13;. second.next  NULL;. cell_t *head  &first;. -first-. 23. 23. next. -second. . -13. next. EPFL",
        "concepts": [
          "Null pointer",
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "T helper cell",
          "Immunotherapy",
          "Typedef",
          "Merkel-cell carcinoma",
          "Type signature",
          "Rust (programming language)",
          "Composite data type",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Function pointer",
          "Computer program",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0045": {
        "ocr_content": " Example. Static. typedef struct _cell. typedef. }. int content;. struct cell* next;. cell_t;. cell_t first, second;. first.content  23;. first.next  &second;. second.content  -13;. second.next  NULL;. cell_t *head  &first;. head. first. . -second. . 23. 23. next. -13. next. EPFL",
        "concepts": [
          "Null pointer",
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "T helper cell",
          "Immunotherapy",
          "Typedef",
          "Merkel-cell carcinoma",
          "Type signature",
          "Rust (programming language)",
          "Composite data type",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Function pointer",
          "Computer program",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0046": {
        "ocr_content": " Example. Static. typedef struct _cell. typedef. int content;. struct cell next;. cell_t;. cell_t first, second;. first.content  23;. first.next  &second;. second.content  -13;. second.next  NULL;. cell_t *head  &first;. head. -first-. second. . 23. 23. next. -13. next. EPFL",
        "concepts": [
          "Null pointer",
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "T helper cell",
          "Immunotherapy",
          "Typedef",
          "Merkel-cell carcinoma",
          "Type signature",
          "Rust (programming language)",
          "Composite data type",
          "Pointer (computer programming)",
          "Union type",
          "Record (computer science)",
          "Function pointer",
          "Computer program",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0047": {
        "ocr_content": " Example. Dynamic. typedef struct _cell. int content;. struct _cell* next;. cell_t;. cell_t *head  NULL;. head  malloc(sizeof(cell_t));. head->content  23;. head->next  NULL;. head. 23. 23. next. EPFL",
        "concepts": [
          "Dynamic loading",
          "Tail call",
          "Null pointer",
          "T cell",
          "Linked list",
          "OpenBSD",
          "T helper cell",
          "Type signature",
          "Rust (programming language)",
          "Dynamic linker",
          "Dynamic range",
          "Sizeof",
          "Pointer (computer programming)",
          "High dynamic range",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0048": {
        "ocr_content": " Example. Dynamic. typedef struct _cell. int content;. struct cell* next;. cell_t;. cell_t *head  NULL;. head  malloc(sizeof(cell_t));. head->content  23;. head->next  NULL;. head. 23. next. EPFL",
        "concepts": [
          "Dynamic loading",
          "Tail call",
          "Null pointer",
          "T cell",
          "Linked list",
          "OpenBSD",
          "T helper cell",
          "Type signature",
          "Rust (programming language)",
          "Dynamic linker",
          "Dynamic range",
          "Sizeof",
          "Pointer (computer programming)",
          "High dynamic range",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0049": {
        "ocr_content": " Example. Dynamic. typedef struct _cell. {. int content;. struct cell* next;. cell_t;. cell_t *head  NULL;. head  malloc(sizeof(cell_t));. head->content  23;. head->next  NULL;. head. 223. head->next  malloc(sizeof(cell_t));. next. ←. ?. next. EPFL",
        "concepts": [
          "Dynamic loading",
          "Tail call",
          "Null pointer",
          "T cell",
          "Memory pool",
          "Linked list",
          "T helper cell",
          "Assertion (software development)",
          "Type signature",
          "Rust (programming language)",
          "Dynamic linker",
          "C dynamic memory allocation",
          "Dynamic range",
          "Sizeof",
          "Pointer (computer programming)",
          "High dynamic range",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Operators in C and C++",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0050": {
        "ocr_content": " head->next. 23. next. →. ?. next. Example. Dynamic. typedef struct _cell. int content;. struct _cell* next;. cell_t;. cell_t *head  NULL;. head  malloc(sizeof(cell_t));. head->content  23;. head->next  NULL;. head. head->next  malloc(sizeof(cell_t));. EPFL",
        "concepts": [
          "Dynamic loading",
          "Tail call",
          "Null pointer",
          "T cell",
          "Memory pool",
          "Linked list",
          "T helper cell",
          "Assertion (software development)",
          "Type signature",
          "Rust (programming language)",
          "Dynamic linker",
          "C dynamic memory allocation",
          "Dynamic range",
          "Sizeof",
          "Pointer (computer programming)",
          "High dynamic range",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Operators in C and C++",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0051": {
        "ocr_content": " head->next. 23. next. -13. next. Example. Dynamic. typedef struct _cell. int content;. struct _cell* next;. cell_t;. cell_t *head  NULL;. head  malloc(sizeof(cell_t));. head->content  23;. head->next  NULL;. head. 23. head->next  malloc(sizeof(cell_t));. head->next->content  -13;. EPFL. #he",
        "concepts": [
          "Dynamic loading",
          "Tail call",
          "Null pointer",
          "T cell",
          "Memory pool",
          "Linked list",
          "T helper cell",
          "Assertion (software development)",
          "Type signature",
          "Rust (programming language)",
          "Dynamic linker",
          "C dynamic memory allocation",
          "Dynamic range",
          "Sizeof",
          "Pointer (computer programming)",
          "High dynamic range",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Operators in C and C++",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0052": {
        "ocr_content": " head->next. 23. next. -13. next. Example. Dynamic. typedef struct _cell. int content;. struct cell* next;. cell_t;. cell_t *head  NULL;. head  malloc(sizeof(cell_t));. head->content  23;. head->next  NULL;. head. head->next  malloc(sizeof(cell_t));. head->next->content  -13;. head->next->next  NULL;. EPFL",
        "concepts": [
          "Dynamic loading",
          "Tail call",
          "Null pointer",
          "T cell",
          "Memory pool",
          "Linked list",
          "T helper cell",
          "Assertion (software development)",
          "Type signature",
          "Rust (programming language)",
          "Dynamic linker",
          "C dynamic memory allocation",
          "Dynamic range",
          "Sizeof",
          "Pointer (computer programming)",
          "High dynamic range",
          "Union type",
          "Record (computer science)",
          "Computer program",
          "Operators in C and C++",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0053": {
        "ocr_content": " Head of the list. head. 23. next. -13. next. EPFL",
        "concepts": [
          "Head of state",
          "Head-directionality parameter",
          "Head of government",
          "Closed list",
          "Open list",
          "Head (linguistics)"
        ]
      }
    },
    {
      "0_zxdz7z5f-0054": {
        "ocr_content": " End of list. head. 23. next. -13. next. EPFL",
        "concepts": [
          "Head of state",
          "Head-directionality parameter",
          "Head of government",
          "Closed list",
          "Open list",
          "Head (linguistics)"
        ]
      }
    },
    {
      "0_zxdz7z5f-0055": {
        "ocr_content": " End of list. head. We want to expand the list. 23. 23. next. -13. next. EPFL",
        "concepts": [
          "Head of state",
          "Head-directionality parameter",
          "Head of government",
          "Closed list",
          "Open list",
          "Head (linguistics)"
        ]
      }
    },
    {
      "0_zxdz7z5f-0056": {
        "ocr_content": " End of list. head. We want to expand the list. One way to do it:. 23. next. -13. next. Search from head until next is NULL. EPFL",
        "concepts": [
          "Null pointer",
          "Yahoo! Search",
          "Linked list",
          "Search algorithm",
          "Search engine",
          "Binary search algorithm"
        ]
      }
    },
    {
      "0_zxdz7z5f-0057": {
        "ocr_content": " Put everything together. EPFL",
        "concepts": [
          "Put option",
          "École Polytechnique Fédérale de Lausanne"
        ]
      }
    },
    {
      "0_zxdz7z5f-0058": {
        "ocr_content": " Put everything together. typedef struct _list. {. cell_t *head;. cell_t *last;. list_t;. EPFL. FF",
        "concepts": [
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "T helper cell",
          "Immunotherapy",
          "C preprocessor",
          "Abstract data type",
          "Typedef",
          "Merkel-cell carcinoma",
          "C syntax",
          "C++11",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0059": {
        "ocr_content": " Put everything together. typedef struct _list. typedef. cell_t *head;. cell_t *last;. list_t;. We can define the empty list:. const list_t list_empty  NULL, NULL;. EPFL",
        "concepts": [
          "Null pointer",
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "T helper cell",
          "Linked data structure",
          "Immunotherapy",
          "C preprocessor",
          "Abstract data type",
          "Typedef",
          "Merkel-cell carcinoma",
          "C syntax",
          "Pointer (computer programming)",
          "Function pointer",
          "C++11",
          "Zig (programming language)",
          "ALGOL 68",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0060": {
        "ocr_content": " Put everything together. typedef struct _list. typedef. cell_t *head;. cell_t *last;. list_t;. We can define the empty list:. const list_t list_empty  NULL, NULL;. Whenever you want to initialize a list, you should do so. EPFL. 1",
        "concepts": [
          "Null pointer",
          "Dendritic cell",
          "Langerhans cell",
          "T cell",
          "Linked list",
          "T helper cell",
          "Linked data structure",
          "Immunotherapy",
          "C preprocessor",
          "Abstract data type",
          "Typedef",
          "Merkel-cell carcinoma",
          "C syntax",
          "Pointer (computer programming)",
          "Function pointer",
          "C++11",
          "Zig (programming language)",
          "ALGOL 68",
          "Head and neck cancer",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0061": {
        "ocr_content": " A new data structure! . EPFL",
        "concepts": [
          "Purely functional data structure",
          "Persistent data structure",
          "Data structure"
        ]
      }
    },
    {
      "0_zxdz7z5f-0062": {
        "ocr_content": " A new data structure! . O. Chained list  linked list. Features:. + Can grow and shrink easily. + Insertion of an element in constant time (even in the middle!). - Linear time in size to access an arbitrary element. EPFL",
        "concepts": [
          "Purely functional data structure",
          "Hash table",
          "Linear time-invariant system",
          "Identity element",
          "Electrical element",
          "Linear model",
          "Linked list",
          "Volume element",
          "Planck constant",
          "Time constant",
          "Linked data structure",
          "Physical constant",
          "Zero element",
          "Implicit data structure",
          "Time complexity",
          "Linear programming",
          "Inverse element",
          "Linear regression",
          "Mathematical constant",
          "Gas constant",
          "Persistent data structure",
          "Linear system",
          "Data structure",
          "Line element"
        ]
      }
    },
    {
      "0_zxdz7z5f-0063": {
        "ocr_content": " Operations. void insert_head (list_t *plist, int value);. int delete_head (list_t *plist);. int_after(list_t *plist, cell_t *where, int value);. int delete_after (list_t *plist, cell_t *where);. cell_t* find_first (const list_t *plist, int value);. void delete_list (list_t *plist);. EPFL",
        "concepts": [
          "CAR T cell",
          "Cancer immunotherapy",
          "T cell",
          "Operations management",
          "T helper cell",
          "C data types",
          "Immortalised cell line",
          "Cell (biology)",
          "Operations research",
          "C syntax",
          "Pointer (computer programming)",
          "Hematopoietic stem cell transplantation",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0064": {
        "ocr_content": " Operations. void insert_head (list_t *plist, int value);. int delete_head (list_t *plist);. int insert_after (list_t *plist, cell_t *where, int value);. int delete_after (list_t *plist, cell_t *where);. cell_t* find_first (const list_t *plist, int value);. void delete_list (list_t *plist);. EPFL",
        "concepts": [
          "CAR T cell",
          "Cancer immunotherapy",
          "T cell",
          "Operations management",
          "T helper cell",
          "Immortalised cell line",
          "Cell (biology)",
          "Operations research",
          "C syntax",
          "Pointer (computer programming)",
          "Hematopoietic stem cell transplantation",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0065": {
        "ocr_content": " Operations. void insert_head (list_t *plist, int value);. int delete_head (list_t *plist);. int insert_after (list_t *plist, cell_t *where, int value);. int delete_after (list_t *plist, cell_t *where);. cell_t* find_first (const list_t *plist, int value);. void delete_list (list_t *plist);. EPFL",
        "concepts": [
          "CAR T cell",
          "Cancer immunotherapy",
          "T cell",
          "Operations management",
          "T helper cell",
          "Immortalised cell line",
          "Cell (biology)",
          "Operations research",
          "C syntax",
          "Pointer (computer programming)",
          "Hematopoietic stem cell transplantation",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0066": {
        "ocr_content": " Insert. 1. next. 个. 3. next. EPFL",
        "concepts": [
          "Medication package insert",
          "École Polytechnique Fédérale de Lausanne"
        ]
      }
    },
    {
      "0_zxdz7z5f-0067": {
        "ocr_content": " Insert. where. 1. next. 个. 3. next. new_cell. malloc(sizeof(cell_t));. new_cell->content  2. EPFL",
        "concepts": [
          "Embryonic stem cell",
          "Cell culture",
          "T cell",
          "T helper cell",
          "Plant cell",
          "Stem cell",
          "Cell membrane",
          "Cell division",
          "Cell (biology)",
          "Cell growth",
          "Cell biology",
          "Cell cycle",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0068": {
        "ocr_content": " Insert. where. 1. next. ←. 2. next. new_cell. 3. next. new_cell. malloc(sizeof(cell_t));. new_cell->content 2. EPFL",
        "concepts": [
          "Embryonic stem cell",
          "Cell culture",
          "T cell",
          "T helper cell",
          "Plant cell",
          "Stem cell",
          "Cell membrane",
          "Cell division",
          "Cell (biology)",
          "Cell growth",
          "Cell biology",
          "Cell cycle",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0069": {
        "ocr_content": " Insert. where. 1. next. 2.. next. new cell. 3. next. new cell. malloc(sizeof(cell_t));. new_cell->content 2. EPFL",
        "concepts": [
          "Cell culture",
          "T cell",
          "B cell",
          "T helper cell",
          "Stem cell",
          "Cell membrane",
          "Cell division",
          "Cell (biology)",
          "Cell adhesion",
          "Cell growth",
          "Cell biology",
          "Cell cycle",
          "Naive T cell"
        ]
      }
    },
    {
      "0_zxdz7z5f-0070": {
        "ocr_content": " Insert. Rewiring 1. where. 1. next. 3. next. 2. next. new cell. EPFL",
        "concepts": [
          "Cell culture",
          "Stem cell",
          "Cell (biology)",
          "Cell adhesion",
          "Cell biology",
          "Cell cycle"
        ]
      }
    },
    {
      "0_zxdz7z5f-0071": {
        "ocr_content": " Insert. Rewiring 1. where. 1. next. 3. next. 2. next. new_cell. new_cell->next. where->next;. EPFL",
        "concepts": [
          "Embryonic stem cell",
          "Cell culture",
          "Plant cell",
          "Stem cell",
          "Cell division",
          "Cell (biology)",
          "Cell biology",
          "Cell cycle"
        ]
      }
    },
    {
      "0_zxdz7z5f-0072": {
        "ocr_content": " Insert. Rewiring 1. where. 1. next. 3. next. 2. next. new_cell. new_cell->next  where->next;. where->next  new_cell;. EPFL",
        "concepts": [
          "Embryonic stem cell",
          "Cell culture",
          "Plant cell",
          "Stem cell",
          "Cell division",
          "Cell (biology)",
          "Cell biology",
          "Cell cycle"
        ]
      }
    },
    {
      "0_zxdz7z5f-0073": {
        "ocr_content": " Insert. Rewiring 2. where. 1. next. 3. next. 2. next. new_cell. new_cell->next  where->next;. where->next  new_cell;. EPFL",
        "concepts": [
          "Embryonic stem cell",
          "Cell culture",
          "Plant cell",
          "Stem cell",
          "Cell division",
          "Cell (biology)",
          "Cell biology",
          "Cell cycle"
        ]
      }
    },
    {
      "0_zxdz7z5f-0074": {
        "ocr_content": " Insert. Rewiring 2. where. 1. next. 3. next. 2. next. new_cell. new_cell->next  where->next;. where->next  new_cell;. EPFL",
        "concepts": [
          "Embryonic stem cell",
          "Cell culture",
          "Plant cell",
          "Stem cell",
          "Cell division",
          "Cell (biology)",
          "Cell biology",
          "Cell cycle"
        ]
      }
    },
    {
      "0_zxdz7z5f-0075": {
        "ocr_content": " Delete. Rewiring. 1. next. 2. next. 3. next. where. del cell. where->next  del_cell->next;. free(del_cell);. EPFL",
        "concepts": [
          "Free will",
          "Free will in theology"
        ]
      }
    },
    {
      "0_zxdz7z5f-0076": {
        "ocr_content": " Object Oriented Programming. Object-Oriented Programming (OOP). EPFL",
        "concepts": [
          "Object (computer science)",
          "Interface (object-oriented programming)",
          "Object database",
          "Object-oriented analysis and design",
          "Inheritance (object-oriented programming)",
          "Object-oriented programming",
          "Object-oriented design",
          "Class-based programming"
        ]
      }
    },
    {
      "0_zxdz7z5f-0077": {
        "ocr_content": " Object Oriented Programming. Object-Oriented Programming (OOP). Languages: C++, Java, Scala, Python. EPFL",
        "concepts": [
          "Java syntax",
          "Java applet",
          "Java (programming language)",
          "Java virtual machine",
          "Object (computer science)",
          "Interface (object-oriented programming)",
          "History of Python",
          "Object database",
          "Python (programming language)",
          "Romance languages",
          "Inheritance (object-oriented programming)",
          "Object-oriented programming",
          "Scala (programming language)",
          "Java bytecode",
          "Berber languages",
          "Java (software platform)",
          "Python syntax and semantics",
          "Object-oriented design",
          "Java version history",
          "Class-based programming"
        ]
      }
    }
  ]
}



            """,
        }
    ]
)

print(reply)

# End time + elapsed time
end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"Start time: {start_time}")
print(f"End time: {end_time}")
print(f"Elapsed time (in seconds): {(datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')).total_seconds()}")


# from pydantic import BaseModel
# from graphregistry.adapters.clients.rcp_models import send_llm_request

# class NodeInput(BaseModel):
#     type: str
#     id: str
#     title: str
#     description: str | None = None


# class NodeClassification(BaseModel):
#     object_type: str
#     subtype: str | None
#     confidence: float


# node = NodeInput(
#     type="Course",
#     id="CS-433",
#     title="Machine Learning",
#     description="Course about supervised learning, neural networks, and model evaluation.",
# )

# reply = send_llm_request(
#     messages=[
#         {
#             "role": "system",
#             "content": "You classify university graph nodes. Return only valid JSON.",
#         },
#         {
#             "role": "user",
#             "content": f"""which model are you
# Classify this node:

# {node.model_dump_json(indent=2)}
# """,
#         },
#     ],
#     response_model=NodeClassification,
# )