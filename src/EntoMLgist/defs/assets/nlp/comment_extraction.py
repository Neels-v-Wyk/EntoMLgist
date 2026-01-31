# Attempt to extract insect information from comments
from EntoMLgist.models.database import Comment
import dagster as dg
from sqlmodel import Session, select

# Schema definition - will be used with the extractor resource
# Only extract common names - genus and species will be populated from other sources
ENTITY_DEFINITIONS = {
    "insect_common_name": "Common name of the insect mentioned, if any (e.g., ladybug, carpet beetle, german cockroach, lucilia sericata, danaus plexippus, etc.)",
}
RELATIONS = []

@dg.asset(required_resource_keys={"db_session", "gliner_extractor"}, deps=["populate_post_upvotes", "populate_comments"])
def extract_insect_names_from_comments(context: dg.AssetExecutionContext):
    """Extract insect names from Reddit comments and update the comments table."""
    session: Session = context.resources.db_session
    extractor = context.resources.gliner_extractor
    
    # Get all comments that have not been processed yet
    statement = select(Comment).where(Comment.extracted_name == None)
    comments = session.exec(statement).all()
    processed_count = 0
    
    # Map entity types to comment attributes
    entity_mapping = {
        "insect_common_name": ("extracted_name", "extracted_name_confidence"),
    }
    
    # Create schema once per execution
    schema = (extractor.create_schema()
        .entities(ENTITY_DEFINITIONS)
        .relations(RELATIONS)
    )
    
    for comment in comments:
        context.log.info(f"Extracting insect names from comment {comment.comment_id}")
        
        try:
            results = extractor.extract(text=comment.body, schema=schema, include_confidence=True)
            
            if results and isinstance(results, dict) and results.get('entities'):
                extracted_data = {}
                entities_dict = results['entities']
                
                # GLiNER2 returns entities as a dict: {entity_type: [list_of_entities]}
                # With include_confidence=True, each entity is (text, confidence) tuple
                for entity_type, entity_list in entities_dict.items():
                    if entity_type in entity_mapping and entity_list:
                        # Take the first extracted entity of each type
                        first_entity = entity_list[0] if entity_list else None
                        
                        if first_entity:
                            # Handle tuple format (text, confidence)
                            if isinstance(first_entity, tuple) and len(first_entity) == 2:
                                entity_text, entity_confidence = first_entity
                            # Handle dict format
                            elif isinstance(first_entity, dict):
                                entity_text = first_entity.get('text')
                                entity_confidence = first_entity.get('confidence') or first_entity.get('score')
                            # Handle plain string (fallback)
                            else:
                                entity_text = first_entity
                                entity_confidence = None
                            
                            if entity_text:
                                name_attr, conf_attr = entity_mapping[entity_type]
                                setattr(comment, name_attr, entity_text)
                                setattr(comment, conf_attr, entity_confidence)
                                extracted_data[name_attr] = (entity_text, entity_confidence)
                
                if extracted_data:
                    session.add(comment)
                    session.commit()
                    
                    log_parts = [f"{k}={v[0]} (conf={v[1]:.3f})" if v[1] is not None else f"{k}={v[0]}" 
                                 for k, v in extracted_data.items()]
                    context.log.info(f"Extracted from comment {comment.comment_id}: {', '.join(log_parts)}")
                    processed_count += 1
        
        except Exception as e:
            context.log.error(f"Failed to extract from comment {comment.comment_id}: {e}")
            session.rollback()  # Rollback failed transaction to allow processing to continue

    return dg.MaterializeResult(
        metadata={"processed_comments_count": processed_count}
    )
