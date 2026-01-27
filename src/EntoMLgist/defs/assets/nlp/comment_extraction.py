# Attempt to extract insect information from comments
from gliner2 import GLiNER2
from EntoMLgist.models.database import Comment
import dagster as dg
from sqlmodel import Session, select

# Load model once, use everywhere
extractor = GLiNER2.from_pretrained("fastino/gliner2-base-v1")
# TODO: Add parameterization for different models or custom models

schema = (extractor.create_schema()
    .entities({
        "insect_common_name": "Common name of the insect mentioned, if any (e.g., ladybug, cicada, mite)",
        "insect_genus": "Genus of the insect mentioned, if any (e.g., Coccinella, Cicada)",
        "insect_species": "Species of the insect mentioned, if any (e.g., septempunctata, septendecim)",
    })
        
    .relations(["found_in"])
)

@dg.asset(required_resource_keys={"db_session"})
def extract_insect_names_from_comments(context: dg.AssetExecutionContext):
    """Extract insect names from Reddit comments and update the comments table."""
    session: Session = context.resources.db_session
    
    # Get all comments that have not been processed yet
    statement = select(Comment).where(Comment.extracted_name == None)
    comments = session.exec(statement).all()
    
    for comment in comments:
        context.log.info(f"Extracting insect names from comment {comment.comment_id}")
        
        try:
            results = extractor.extract(
                text=[comment.body],
                schema=schema,
            )
            
            insect_name = None
            insect_genus = None
            insect_species = None
            
            entities = results[0].entities
            for entity in entities:
                if entity.type == "insect_common_name":
                    insect_name = entity.text
                elif entity.type == "insect_genus":
                    insect_genus = entity.text
                elif entity.type == "insect_species":
                    insect_species = entity.text
            
            comment.extracted_name = insect_name
            comment.extracted_genus = insect_genus
            comment.extracted_species = insect_species
            
            session.add(comment)
            session.commit()
            
            context.log.info(f"Extracted from comment {comment.comment_id}: name={insect_name}, genus={insect_genus}, species={insect_species}")
        
        except Exception as e:
            context.log.error(f"Error extracting insect names from comment {comment.comment_id}: {e}")
