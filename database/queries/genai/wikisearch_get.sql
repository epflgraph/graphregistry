SELECT suggestions_list
  FROM [[genai]].GenAI_N_Keyword_T_WikiSearch
 WHERE keyword_hash = '[[keyword_hash]]'
   AND record_deleted = 0;
