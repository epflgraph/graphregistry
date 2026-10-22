SELECT output_text
  FROM [[genai]].GenAI_N_Text_T_Wikify
 WHERE input_hash = '[[input_hash]]'
   AND record_deleted = 0;
