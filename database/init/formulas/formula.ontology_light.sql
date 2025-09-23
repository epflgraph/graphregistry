
                  -- List of concepts to keep
        REPLACE INTO [[ontology_light]]._concept_ids
     SELECT DISTINCT concept_id
                FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection;
     
                  -- List of categories to keep
        REPLACE INTO [[ontology_light]]._category_ids
        
              SELECT 'academic-disciplines' AS category_id

               UNION

     SELECT DISTINCT a1.from_id AS category_id
                FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild a1
          INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild a2
                  ON a1.to_id = a2.from_id
          INNER JOIN [[ontology_light]]._concept_ids a3
                  ON a2.to_id = a3.concept_id
        
               UNION
        
     SELECT DISTINCT b4.to_id AS category_id
                FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild b1
          INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild b2
                  ON b1.to_id = b2.from_id
          INNER JOIN [[ontology_light]]._concept_ids b3
                  ON b2.to_id = b3.concept_id
          INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent b4
                  ON b1.from_id = b4.from_id
          
               UNION
               
     SELECT DISTINCT t5.to_id AS category_id
                FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild t1
          INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t2
                  ON t1.to_id = t2.from_id
          INNER JOIN [[ontology_light]]._concept_ids t3
                  ON t2.to_id = t3.concept_id
          INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t4
                  ON t1.from_id = t4.from_id
          INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t5
                  ON t4.to_id = t5.from_id
        
               UNION
        
     SELECT DISTINCT c6.to_id AS category_id
                FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild c1
          INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c2
                  ON c1.to_id = c2.from_id
          INNER JOIN [[ontology_light]]._concept_ids c3
                  ON c2.to_id = c3.concept_id
          INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent c4
                  ON c1.from_id = c4.from_id
          INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent c5
                  ON c4.to_id = c5.from_id
          INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent c6
                  ON c5.to_id = c6.from_id;
              
                  -- TABLE: Data_N_Object_T_CustomFields
        REPLACE INTO [[ontology_light]].Data_N_Object_T_CustomFields
                    (institution_id, object_type, object_id, field_language, field_name, field_value)
              SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                FROM [[ontology]].Data_N_Object_T_CustomFields a
          INNER JOIN [[ontology_light]]._category_ids b
                  ON (a.object_type, a.object_id) = ('Category', b.category_id)
           UNION ALL
              SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                FROM [[ontology]].Data_N_Object_T_CustomFields c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON (c.object_type, c.object_id) = ('Concept', d.concept_id);

                  -- TABLE: Data_N_Object_T_Embeddings
        REPLACE INTO [[ontology_light]].Data_N_Object_T_Embeddings
                    (institution_id, object_type, object_id, embedding)
              SELECT institution_id, object_type, object_id, embedding
                FROM [[ontology]].Data_N_Object_T_Embeddings a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.object_id = b.concept_id;

                  -- TABLE: Data_N_Object_T_PageProfile
        REPLACE INTO [[ontology_light]].Data_N_Object_T_PageProfile
                    (institution_id, object_type, object_id, numeric_id_en, numeric_id_fr, numeric_id_de, numeric_id_it, short_code, subtype_en, subtype_fr, subtype_de, subtype_it, name_en_is_auto_generated, name_en_is_auto_corrected, name_en_is_auto_translated, name_en_translated_from, name_en_value, name_fr_is_auto_generated, name_fr_is_auto_corrected, name_fr_is_auto_translated, name_fr_translated_from, name_fr_value, name_de_is_auto_generated, name_de_is_auto_corrected, name_de_is_auto_translated, name_de_translated_from, name_de_value, name_it_is_auto_generated, name_it_is_auto_corrected, name_it_is_auto_translated, name_it_translated_from, name_it_value, description_short_en_is_auto_generated, description_short_en_is_auto_corrected, description_short_en_is_auto_translated, description_short_en_translated_from, description_short_en_value, description_short_fr_is_auto_generated, description_short_fr_is_auto_corrected, description_short_fr_is_auto_translated, description_short_fr_translated_from, description_short_fr_value, description_short_de_is_auto_generated, description_short_de_is_auto_corrected, description_short_de_is_auto_translated, description_short_de_translated_from, description_short_de_value, description_short_it_is_auto_generated, description_short_it_is_auto_corrected, description_short_it_is_auto_translated, description_short_it_translated_from, description_short_it_value, description_medium_en_is_auto_generated, description_medium_en_is_auto_corrected, description_medium_en_is_auto_translated, description_medium_en_translated_from, description_medium_en_value, description_medium_fr_is_auto_generated, description_medium_fr_is_auto_corrected, description_medium_fr_is_auto_translated, description_medium_fr_translated_from, description_medium_fr_value, description_medium_de_is_auto_generated, description_medium_de_is_auto_corrected, description_medium_de_is_auto_translated, description_medium_de_translated_from, description_medium_de_value, description_medium_it_is_auto_generated, description_medium_it_is_auto_corrected, description_medium_it_is_auto_translated, description_medium_it_translated_from, description_medium_it_value, description_long_en_is_auto_generated, description_long_en_is_auto_corrected, description_long_en_is_auto_translated, description_long_en_translated_from, description_long_en_value, description_long_fr_is_auto_generated, description_long_fr_is_auto_corrected, description_long_fr_is_auto_translated, description_long_fr_translated_from, description_long_fr_value, description_long_de_is_auto_generated, description_long_de_is_auto_corrected, description_long_de_is_auto_translated, description_long_de_translated_from, description_long_de_value, description_long_it_is_auto_generated, description_long_it_is_auto_corrected, description_long_it_is_auto_translated, description_long_it_translated_from, description_long_it_value, external_key_en, external_key_fr, external_key_de, external_key_it, external_url_en, external_url_fr, external_url_de, external_url_it, is_visible)
              SELECT institution_id, object_type, object_id, numeric_id_en, numeric_id_fr, numeric_id_de, numeric_id_it, short_code, subtype_en, subtype_fr, subtype_de, subtype_it, name_en_is_auto_generated, name_en_is_auto_corrected, name_en_is_auto_translated, name_en_translated_from, name_en_value, name_fr_is_auto_generated, name_fr_is_auto_corrected, name_fr_is_auto_translated, name_fr_translated_from, name_fr_value, name_de_is_auto_generated, name_de_is_auto_corrected, name_de_is_auto_translated, name_de_translated_from, name_de_value, name_it_is_auto_generated, name_it_is_auto_corrected, name_it_is_auto_translated, name_it_translated_from, name_it_value, description_short_en_is_auto_generated, description_short_en_is_auto_corrected, description_short_en_is_auto_translated, description_short_en_translated_from, description_short_en_value, description_short_fr_is_auto_generated, description_short_fr_is_auto_corrected, description_short_fr_is_auto_translated, description_short_fr_translated_from, description_short_fr_value, description_short_de_is_auto_generated, description_short_de_is_auto_corrected, description_short_de_is_auto_translated, description_short_de_translated_from, description_short_de_value, description_short_it_is_auto_generated, description_short_it_is_auto_corrected, description_short_it_is_auto_translated, description_short_it_translated_from, description_short_it_value, description_medium_en_is_auto_generated, description_medium_en_is_auto_corrected, description_medium_en_is_auto_translated, description_medium_en_translated_from, description_medium_en_value, description_medium_fr_is_auto_generated, description_medium_fr_is_auto_corrected, description_medium_fr_is_auto_translated, description_medium_fr_translated_from, description_medium_fr_value, description_medium_de_is_auto_generated, description_medium_de_is_auto_corrected, description_medium_de_is_auto_translated, description_medium_de_translated_from, description_medium_de_value, description_medium_it_is_auto_generated, description_medium_it_is_auto_corrected, description_medium_it_is_auto_translated, description_medium_it_translated_from, description_medium_it_value, description_long_en_is_auto_generated, description_long_en_is_auto_corrected, description_long_en_is_auto_translated, description_long_en_translated_from, description_long_en_value, description_long_fr_is_auto_generated, description_long_fr_is_auto_corrected, description_long_fr_is_auto_translated, description_long_fr_translated_from, description_long_fr_value, description_long_de_is_auto_generated, description_long_de_is_auto_corrected, description_long_de_is_auto_translated, description_long_de_translated_from, description_long_de_value, description_long_it_is_auto_generated, description_long_it_is_auto_corrected, description_long_it_is_auto_translated, description_long_it_translated_from, description_long_it_value, external_key_en, external_key_fr, external_key_de, external_key_it, external_url_en, external_url_fr, external_url_de, external_url_it, is_visible
                FROM [[ontology]].Data_N_Object_T_PageProfile a
          INNER JOIN [[ontology_light]]._category_ids b
                  ON (a.object_type, a.object_id) = ('Category', b.category_id)
           UNION ALL
              SELECT institution_id, object_type, object_id, numeric_id_en, numeric_id_fr, numeric_id_de, numeric_id_it, short_code, subtype_en, subtype_fr, subtype_de, subtype_it, name_en_is_auto_generated, name_en_is_auto_corrected, name_en_is_auto_translated, name_en_translated_from, name_en_value, name_fr_is_auto_generated, name_fr_is_auto_corrected, name_fr_is_auto_translated, name_fr_translated_from, name_fr_value, name_de_is_auto_generated, name_de_is_auto_corrected, name_de_is_auto_translated, name_de_translated_from, name_de_value, name_it_is_auto_generated, name_it_is_auto_corrected, name_it_is_auto_translated, name_it_translated_from, name_it_value, description_short_en_is_auto_generated, description_short_en_is_auto_corrected, description_short_en_is_auto_translated, description_short_en_translated_from, description_short_en_value, description_short_fr_is_auto_generated, description_short_fr_is_auto_corrected, description_short_fr_is_auto_translated, description_short_fr_translated_from, description_short_fr_value, description_short_de_is_auto_generated, description_short_de_is_auto_corrected, description_short_de_is_auto_translated, description_short_de_translated_from, description_short_de_value, description_short_it_is_auto_generated, description_short_it_is_auto_corrected, description_short_it_is_auto_translated, description_short_it_translated_from, description_short_it_value, description_medium_en_is_auto_generated, description_medium_en_is_auto_corrected, description_medium_en_is_auto_translated, description_medium_en_translated_from, description_medium_en_value, description_medium_fr_is_auto_generated, description_medium_fr_is_auto_corrected, description_medium_fr_is_auto_translated, description_medium_fr_translated_from, description_medium_fr_value, description_medium_de_is_auto_generated, description_medium_de_is_auto_corrected, description_medium_de_is_auto_translated, description_medium_de_translated_from, description_medium_de_value, description_medium_it_is_auto_generated, description_medium_it_is_auto_corrected, description_medium_it_is_auto_translated, description_medium_it_translated_from, description_medium_it_value, description_long_en_is_auto_generated, description_long_en_is_auto_corrected, description_long_en_is_auto_translated, description_long_en_translated_from, description_long_en_value, description_long_fr_is_auto_generated, description_long_fr_is_auto_corrected, description_long_fr_is_auto_translated, description_long_fr_translated_from, description_long_fr_value, description_long_de_is_auto_generated, description_long_de_is_auto_corrected, description_long_de_is_auto_translated, description_long_de_translated_from, description_long_de_value, description_long_it_is_auto_generated, description_long_it_is_auto_corrected, description_long_it_is_auto_translated, description_long_it_translated_from, description_long_it_value, external_key_en, external_key_fr, external_key_de, external_key_it, external_url_en, external_url_fr, external_url_de, external_url_it, is_visible
                FROM [[ontology]].Data_N_Object_T_PageProfile c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON (c.object_type, c.object_id) = ('Concept', d.concept_id);

                  -- TABLE: Edges_N_Category_N_Category_T_ChildToParent
        REPLACE INTO [[ontology_light]].Edges_N_Category_N_Category_T_ChildToParent
                     (from_id, to_id)
     SELECT DISTINCT a.from_id, a.to_id
                FROM [[ontology]].Edges_N_Category_N_Category_T_ChildToParent a
          INNER JOIN [[ontology_light]]._category_ids b
                  ON a.from_id = b.category_id OR a.to_id = b.category_id;
            
                  -- TABLE: Edges_N_Category_N_ConceptsCluster_T_ParentToChild
        REPLACE INTO [[ontology_light]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild
                     (from_id, to_id)
     SELECT DISTINCT a.from_id, a.to_id
                FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild a
          INNER JOIN [[ontology_light]]._category_ids b
                  ON a.from_id = b.category_id OR a.to_id = b.category_id;
                  
                  -- TABLE: Edges_N_Category_N_Concept_T_AnchorPage
        REPLACE INTO [[ontology_light]].Edges_N_Category_N_Concept_T_AnchorPage
                     (from_id, to_id)
              SELECT a.from_id, a.to_id
                FROM [[ontology]].Edges_N_Category_N_Concept_T_AnchorPage a
          INNER JOIN [[ontology_light]]._category_ids b
                  ON a.from_id = b.category_id
               UNION
              SELECT c.from_id, c.to_id
                FROM [[ontology]].Edges_N_Category_N_Concept_T_AnchorPage c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON c.to_id = d.concept_id;

                  -- TABLE: Edges_N_Concept_N_Concept_T_Directed (from total of 20'119'292)
        REPLACE INTO [[ontology_light]].Edges_N_Concept_N_Concept_T_Directed
                     (from_id, to_id)
              SELECT a.from_id, a.to_id
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Directed a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.from_id = b.concept_id
               UNION
              SELECT c.from_id, c.to_id
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Directed c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON c.to_id = d.concept_id;
      
                  -- TABLE: Edges_N_Concept_N_Concept_T_Embeddings (from total of 192'581'518) 
        REPLACE INTO [[ontology_light]].Edges_N_Concept_N_Concept_T_Embeddings
                     (from_id, to_id, score)
              SELECT a.from_id, a.to_id, a.score
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Embeddings a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.from_id = b.concept_id
               UNION
              SELECT c.from_id, c.to_id, c.score
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Embeddings c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON c.to_id = d.concept_id;
      
                  -- TABLE: Edges_N_Concept_N_Concept_T_Symmetric (from total of 3'100'436)
        REPLACE INTO [[ontology_light]].Edges_N_Concept_N_Concept_T_Symmetric
                     (id, from_id, to_id, score)
              SELECT a.id, a.from_id, a.to_id, a.score
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Symmetric a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.from_id = b.concept_id
               UNION
              SELECT c.id, c.from_id, c.to_id, c.score
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Symmetric c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON c.to_id = d.concept_id;
      
                  -- TABLE: Edges_N_Concept_N_Concept_T_Symmetric (from total of 1'497'301)
        REPLACE INTO [[ontology_light]].Edges_N_Concept_N_Concept_T_Undirected
                     (from_id, to_id, score, normalised_score)
              SELECT a.from_id, a.to_id, a.score, a.normalised_score
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Undirected a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.from_id = b.concept_id
               UNION
              SELECT c.from_id, c.to_id, c.score, c.normalised_score
                FROM [[ontology]].Edges_N_Concept_N_Concept_T_Undirected c
          INNER JOIN [[ontology_light]]._concept_ids d
                  ON c.to_id = d.concept_id;
      
                  -- TABLE: Edges_N_ConceptsCluster_N_Concept_T_ParentToChild
        REPLACE INTO [[ontology_light]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild
                     (from_id, to_id)
     SELECT DISTINCT a.from_id, a.to_id
                FROM [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.to_id = b.concept_id;
                  
                  -- TABLE: Nodes_N_Category
        REPLACE INTO [[ontology_light]].Nodes_N_Category
                    (institution_id, object_type, object_id, id, name, depth, reference_page_id, reference_page_key, reference_page_url)
     SELECT DISTINCT institution_id, object_type, object_id, id, name, depth, reference_page_id, reference_page_key, reference_page_url
                FROM [[ontology]].Nodes_N_Category a
          INNER JOIN [[ontology_light]]._category_ids b
                  ON a.object_id = b.category_id;
                  
                  -- TABLE: Nodes_N_Concept
        REPLACE INTO [[ontology_light]].Nodes_N_Concept
                    (institution_id, object_type, object_id, id, name, is_ontology_category, is_ontology_concept, is_ontology_neighbour, is_noise, is_unused)
     SELECT DISTINCT institution_id, object_type, object_id, id, name, is_ontology_category, is_ontology_concept, is_ontology_neighbour, is_noise, is_unused
                FROM [[ontology]].Nodes_N_Concept a
          INNER JOIN [[ontology_light]]._concept_ids b
                  ON a.object_id = b.concept_id;

                    -- (Re)generate edge views
CREATE OR REPLACE VIEW [[ontology_light]].Edges_N_Object_N_Object_T_ChildToParent AS
                SELECT 'Ont' AS from_institution_id, 'Concept'  AS from_object_type,   t1.to_id AS from_object_id,
                       'Ont' AS   to_institution_id, 'Category' AS   to_object_type, t2.from_id AS   to_object_id,
                       'ontology tree' AS context, t1.row_id
                  FROM [[ontology_light]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t1
            INNER JOIN [[ontology_light]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild t2
                    ON t1.from_id = t2.to_id
             UNION ALL
                SELECT 'Ont' AS from_institution_id, 'Category' AS from_object_type, from_id AS from_object_id,
                       'Ont' AS   to_institution_id, 'Category' AS   to_object_type,   to_id AS   to_object_id,
                       'ontology tree' AS context, row_id
                  FROM [[ontology_light]].Edges_N_Category_N_Category_T_ChildToParent;

                    -- (Re)generate node views
CREATE OR REPLACE VIEW [[ontology_light]].Nodes_N_Object AS
                SELECT institution_id, object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text, row_id
                  FROM [[ontology_light]].Nodes_N_Concept
             UNION ALL  
                SELECT institution_id, object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text, row_id
                  FROM [[ontology_light]].Nodes_N_Category;