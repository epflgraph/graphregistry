SELECT call_role, chunk_num, output_text
  FROM [[genai]].GenAI_N_Object_T_CachedCompletions
 WHERE object_type = '[[object_type]]'
   AND object_id = '[[object_id]]'
   AND model_used = '[[model_used]]'
   AND input_hash = '[[input_hash]]'
   AND record_deleted = 0;
