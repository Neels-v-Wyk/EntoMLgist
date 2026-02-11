"""Enrich normalized insect names with taxonomic information from GBIF."""
import json
from datetime import datetime
from typing import Optional
import dagster as dg
from sqlmodel import Session, select, func
from pygbif import species
from EntoMLgist.models.database import Comment, TaxonomicName, ImageUrl, ImageTaxonomyLink


def query_gbif_taxonomy(common_name: str, context: dg.AssetExecutionContext) -> Optional[dict]:
    """Query GBIF API for taxonomic information based on common name.
    
    Args:
        common_name: Normalized common name to search for
        context: Dagster execution context for logging
        
    Returns:
        Dictionary with taxonomic information or None if lookup fails
    """
    try:
        context.log.info(f"Querying GBIF for: {common_name}")
        
        # Search for name match in GBIF backbone taxonomy
        # Note: pygbif uses 'scientificName' parameter for both common and scientific names
        name_match = species.name_backbone(
            scientificName=common_name,
            strict=False,  # Allow fuzzy matching
            verbose=True,
            kingdom='Animalia'  # Restrict to animals
        )
        
        if not name_match:
            context.log.warning(f"No GBIF match found for: {common_name}")
            return None
        
        context.log.debug(f"GBIF match for '{common_name}': {name_match}")
        
        # Extract taxonomic information - handle both old and new API response formats
        # New format has data nested under 'usage' and 'diagnostics' keys
        usage_data = name_match.get('usage', name_match)  # Fallback to root if 'usage' not present
        diagnostics = name_match.get('diagnostics', {})
        
        # For classification fields, check the classification array if needed
        classification = name_match.get('classification', [])
        classification_dict = {}
        for item in classification:
            rank = item.get('rank', '').lower()
            classification_dict[rank] = item.get('name')
        
        result = {
            'gbif_usage_key': usage_data.get('key') or name_match.get('usageKey'),
            'gbif_accepted_key': name_match.get('acceptedUsageKey'),
            'gbif_taxonomic_status': usage_data.get('status') or name_match.get('status'),
            'gbif_match_type': diagnostics.get('matchType') or name_match.get('matchType'),
            'gbif_confidence': diagnostics.get('confidence') or name_match.get('confidence'),
            'scientific_name': usage_data.get('name') or usage_data.get('scientificName') or name_match.get('scientificName'),
            'canonical_name': usage_data.get('canonicalName') or name_match.get('canonicalName'),
            'kingdom': classification_dict.get('kingdom') or usage_data.get('kingdom') or name_match.get('kingdom'),
            'phylum': classification_dict.get('phylum') or usage_data.get('phylum') or name_match.get('phylum'),
            'class_name': classification_dict.get('class') or usage_data.get('class') or name_match.get('class'),
            'order': classification_dict.get('order') or usage_data.get('order') or name_match.get('order'),
            'family': classification_dict.get('family') or usage_data.get('family') or name_match.get('family'),
            'genus': classification_dict.get('genus') or usage_data.get('genus') or name_match.get('genus'),
            'species': classification_dict.get('species') or usage_data.get('species') or name_match.get('species'),
            'rank': usage_data.get('rank') or name_match.get('rank'),
            'lookup_success': True,
            'lookup_timestamp': int(datetime.now().timestamp()),
        }
        
        # Verify it's actually an insect (class Insecta)
        class_value = result.get('class_name') or ''
        result['is_insect'] = class_value == 'Insecta'
        
        if not result['is_insect']:
            context.log.warning(
                f"'{common_name}' matched to class '{class_value}', not Insecta. "
                f"Scientific name: {result['scientific_name']}"
            )
        
        # Optionally fetch vernacular (common) names if we have a usage key
        if result['gbif_usage_key']:
            try:
                vernacular = species.name_usage(
                    key=result['gbif_usage_key'],
                    data='vernacularNames',
                    limit=20
                )
                
                if vernacular and 'results' in vernacular:
                    # Extract vernacular names, preferring English
                    names = []
                    english_names = []
                    
                    for v in vernacular['results']:
                        vname = v.get('vernacularName', '').lower()
                        lang = v.get('language', '')
                        
                        if vname:
                            if lang == 'eng' or lang == 'en':
                                english_names.append(vname)
                            else:
                                names.append(vname)
                    
                    # Prefer English names, then others
                    all_names = english_names + names
                    if all_names:
                        result['vernacular_names'] = json.dumps(all_names[:10])  # Limit to 10
                        context.log.info(f"Found {len(all_names)} vernacular names for {common_name}")
            
            except Exception as e:
                context.log.warning(f"Could not fetch vernacular names for {common_name}: {e}")
        
        # Try to get occurrence count (indicates data availability)
        try:
            if result['gbif_usage_key']:
                occurrence_search = species.name_usage(
                    key=result['gbif_usage_key'],
                    data='metrics'
                )
                if occurrence_search and 'numOccurrences' in occurrence_search:
                    result['num_occurrences'] = occurrence_search['numOccurrences']
        except Exception as e:
            context.log.warning(f"Could not fetch occurrence count for {common_name}: {e}")
        
        return result
        
    except Exception as e:
        context.log.error(f"GBIF API error for '{common_name}': {e}")
        return {
            'lookup_success': False,
            'lookup_error': str(e),
            'lookup_timestamp': int(datetime.now().timestamp()),
        }


@dg.asset(required_resource_keys={"db_session"}, deps=["normalize_insect_names"])
def enrich_taxonomy_from_gbif(context: dg.AssetExecutionContext):
    """Query GBIF for taxonomic information for all unique normalized insect names."""
    session: Session = context.resources.db_session
    
    # Get all unique normalized insect names from comments
    unique_names_query = (
        select(Comment.extracted_name)
        .where(Comment.extracted_name.isnot(None))
        .distinct()
    )
    unique_names = session.exec(unique_names_query).all()
    
    context.log.info(f"Found {len(unique_names)} unique insect names to enrich")
    
    # Track statistics
    new_lookups = 0
    updated_lookups = 0
    successful_lookups = 0
    insect_matches = 0
    non_insect_matches = 0
    failed_lookups = 0
    
    for common_name in unique_names:
        # Check if we already have taxonomy data for this name
        existing = session.get(TaxonomicName, common_name)
        
        # Skip if we have a successful lookup, but retry if it failed before
        #if existing and existing.lookup_success:
            #context.log.debug(f"Skipping '{common_name}' - already enriched")
            # continue
            # temporary bypass to recheck all names
        
        # Log if we're retrying a failed lookup
        if existing and not existing.lookup_success:
            context.log.info(f"Retrying failed lookup for '{common_name}'")
        
        # Query GBIF
        taxonomy_data = query_gbif_taxonomy(common_name, context)
        
        if taxonomy_data:
            if existing:
                # Update existing record
                for key, value in taxonomy_data.items():
                    setattr(existing, key, value)
                updated_lookups += 1
            else:
                # Create new record
                taxonomy_record = TaxonomicName(
                    common_name=common_name,
                    **taxonomy_data
                )
                session.add(taxonomy_record)
                new_lookups += 1
            
            if taxonomy_data.get('lookup_success'):
                successful_lookups += 1
                if taxonomy_data.get('is_insect'):
                    insect_matches += 1
                else:
                    non_insect_matches += 1
            else:
                failed_lookups += 1
        else:
            # Record failed lookup
            if existing:
                existing.lookup_success = False
                existing.lookup_timestamp = int(datetime.now().timestamp())
                updated_lookups += 1
            else:
                taxonomy_record = TaxonomicName(
                    common_name=common_name,
                    lookup_success=False,
                    lookup_timestamp=int(datetime.now().timestamp())
                )
                session.add(taxonomy_record)
                new_lookups += 1
            failed_lookups += 1
        
        # Commit periodically to avoid losing progress
        if (new_lookups + updated_lookups) % 10 == 0:
            session.commit()
            context.log.info(f"Progress: {new_lookups + updated_lookups}/{len(unique_names)} names processed")
    
    session.commit()
    
    context.log.info(
        f"Taxonomy enrichment complete: {new_lookups} new, {updated_lookups} updated, "
        f"{successful_lookups} successful, {failed_lookups} failed"
    )
    
    # Generate metadata for Dagster UI
    metadata = {
        "unique_names": dg.MetadataValue.int(len(unique_names)),
        "new_lookups": dg.MetadataValue.int(new_lookups),
        "updated_lookups": dg.MetadataValue.int(updated_lookups),
        "successful_lookups": dg.MetadataValue.int(successful_lookups),
        "failed_lookups": dg.MetadataValue.int(failed_lookups),
        "insect_matches": dg.MetadataValue.int(insect_matches),
        "non_insect_matches": dg.MetadataValue.int(non_insect_matches),
    }
    
    # Query top enriched taxa by occurrence (including failed lookups for visibility)
    top_taxa_query = (
        select(
            TaxonomicName.common_name,
            TaxonomicName.scientific_name,
            TaxonomicName.order,
            TaxonomicName.family,
            TaxonomicName.rank,
            TaxonomicName.is_insect,
            TaxonomicName.num_occurrences,
            TaxonomicName.lookup_success,
            TaxonomicName.lookup_error,
            func.count(Comment.comment_id).label('mention_count')
        )
        .join(Comment, Comment.extracted_name == TaxonomicName.common_name)
        .group_by(
            TaxonomicName.common_name,
            TaxonomicName.scientific_name,
            TaxonomicName.order,
            TaxonomicName.family,
            TaxonomicName.rank,
            TaxonomicName.is_insect,
            TaxonomicName.num_occurrences,
            TaxonomicName.lookup_success,
            TaxonomicName.lookup_error
        )
        .order_by(func.count(Comment.comment_id).desc())
        .limit(20)
    )
    
    top_taxa = session.exec(top_taxa_query).all()
    
    if top_taxa:
        markdown_rows = [
            "| Common Name | Scientific Name | Order | Family | Rank | Insect? | Mentions | GBIF Occurrences | Status |",
            "|-------------|-----------------|-------|--------|------|---------|----------|------------------|--------|"
        ]
        
        for (common, scientific, order, family, rank, is_insect, occurrences, lookup_success, lookup_error, mentions) in top_taxa:
            insect_flag = "✓" if is_insect else "✗"
            scientific_display = scientific or "N/A"
            order_display = order or "N/A"
            family_display = family or "N/A"
            rank_display = rank or "N/A"
            occurrences_display = f"{occurrences:,}" if occurrences else "N/A"
            
            # Show status - successful or error
            if lookup_success:
                status = "✓"
            elif lookup_error:
                # Truncate error message
                error_msg = lookup_error[:30] + "..." if len(lookup_error) > 30 else lookup_error
                status = f"❌ {error_msg}"
            else:
                status = "⏳ Pending"
            
            markdown_rows.append(
                f"| {common} | {scientific_display} | {order_display} | {family_display} | "
                f"{rank_display} | {insect_flag} | {mentions} | {occurrences_display} | {status} |"
            )
        
        metadata["top_20_enriched_taxa"] = dg.MetadataValue.md("\n".join(markdown_rows))
    
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(required_resource_keys={"db_session"}, deps=["enrich_taxonomy_from_gbif", "get_image_uris_from_posts"])
def link_images_to_taxonomy(context: dg.AssetExecutionContext):
    """Create links between images and their taxonomic classifications based on comments."""
    session: Session = context.resources.db_session
    
    # Get all comments with extracted names that also have associated images
    comments_with_names_query = (
        select(Comment, ImageUrl)
        .join(ImageUrl, Comment.parent_post_id == ImageUrl.parent_post_id)
        .where(Comment.extracted_name.isnot(None))
    )
    
    comment_image_pairs = session.exec(comments_with_names_query).all()
    
    context.log.info(f"Found {len(comment_image_pairs)} comment-image pairs to link")
    
    new_links = 0
    updated_links = 0
    skipped_links = 0
    
    for comment, image in comment_image_pairs:
        # Check if this taxonomy exists
        taxonomy = session.get(TaxonomicName, comment.extracted_name)
        
        if not taxonomy:
            context.log.warning(
                f"No taxonomy found for '{comment.extracted_name}' in comment {comment.comment_id}"
            )
            skipped_links += 1
            continue
        
        # Calculate combined label quality score
        # Combines GLiNER extraction confidence (0-1) with GBIF match confidence (0-100)
        label_quality = None
        if comment.extracted_name_confidence is not None and taxonomy.gbif_confidence is not None:
            # Normalize GBIF confidence to 0-1 range and multiply with extraction confidence
            label_quality = comment.extracted_name_confidence * (taxonomy.gbif_confidence / 100.0)
        elif comment.extracted_name_confidence is not None:
            # If only extraction confidence available, use that
            label_quality = comment.extracted_name_confidence
        elif taxonomy.gbif_confidence is not None:
            # If only GBIF confidence available, use that normalized
            label_quality = taxonomy.gbif_confidence / 100.0
        
        # Check if link already exists
        existing_link = session.get(ImageTaxonomyLink, (image.image_id, comment.extracted_name))
        
        if existing_link:
            # Update if any confidence values changed
            updated = False
            if existing_link.extraction_confidence != comment.extracted_name_confidence:
                existing_link.extraction_confidence = comment.extracted_name_confidence
                updated = True
            if existing_link.gbif_confidence != taxonomy.gbif_confidence:
                existing_link.gbif_confidence = taxonomy.gbif_confidence
                updated = True
            if existing_link.label_quality_score != label_quality:
                existing_link.label_quality_score = label_quality
                updated = True
            
            if updated:
                updated_links += 1
            else:
                skipped_links += 1
            continue
        
        # Create new link
        link = ImageTaxonomyLink(
            image_id=image.image_id,
            common_name=comment.extracted_name,
            comment_id=comment.comment_id,
            extraction_confidence=comment.extracted_name_confidence,
            gbif_confidence=taxonomy.gbif_confidence,
            label_quality_score=label_quality,
            comment_upvotes=comment.upvotes,
            is_top_identification=False  # Will be computed in a future asset
        )
        
        session.add(link)
        new_links += 1
        
        # Commit periodically
        if new_links % 100 == 0:
            session.commit()
            context.log.info(f"Progress: {new_links} links created")
    
    session.commit()
    
    context.log.info(
        f"Image-taxonomy linking complete: {new_links} new links, "
        f"{updated_links} updated, {skipped_links} skipped"
    )
    
    # Generate statistics
    metadata = {
        "total_pairs": dg.MetadataValue.int(len(comment_image_pairs)),
        "new_links": dg.MetadataValue.int(new_links),
        "updated_links": dg.MetadataValue.int(updated_links),
        "skipped_links": dg.MetadataValue.int(skipped_links),
    }
    
    # Calculate quality score statistics
    quality_stats = session.exec(
        select(
            func.count(ImageTaxonomyLink.image_id).label('total_links'),
            func.avg(ImageTaxonomyLink.label_quality_score).label('avg_quality'),
            func.min(ImageTaxonomyLink.label_quality_score).label('min_quality'),
            func.max(ImageTaxonomyLink.label_quality_score).label('max_quality')
        )
        .where(ImageTaxonomyLink.label_quality_score.isnot(None))
    ).one()
    
    if quality_stats and quality_stats[0] > 0:
        total_links, avg_quality, min_quality, max_quality = quality_stats
        metadata["links_with_quality_score"] = dg.MetadataValue.int(total_links)
        metadata["avg_label_quality"] = dg.MetadataValue.float(round(avg_quality, 3))
        metadata["min_label_quality"] = dg.MetadataValue.float(round(min_quality, 3))
        metadata["max_label_quality"] = dg.MetadataValue.float(round(max_quality, 3))
        
        # Count high-quality labels (>0.7)
        high_quality_count = session.exec(
            select(func.count(ImageTaxonomyLink.image_id))
            .where(ImageTaxonomyLink.label_quality_score >= 0.7)
        ).one()
        metadata["high_quality_labels"] = dg.MetadataValue.int(high_quality_count)
        metadata["high_quality_percentage"] = dg.MetadataValue.float(
            round(high_quality_count / total_links * 100, 1)
        )
    
    # Query image-taxonomy distribution
    taxonomy_distribution_query = (
        select(
            TaxonomicName.common_name,
            TaxonomicName.scientific_name,
            TaxonomicName.order,
            func.count(ImageTaxonomyLink.image_id.distinct()).label('image_count')
        )
        .join(ImageTaxonomyLink, ImageTaxonomyLink.common_name == TaxonomicName.common_name)
        .where(TaxonomicName.is_insect == True)
        .group_by(
            TaxonomicName.common_name,
            TaxonomicName.scientific_name,
            TaxonomicName.order
        )
        .order_by(func.count(ImageTaxonomyLink.image_id.distinct()).desc())
        .limit(20)
    )
    
    distribution = session.exec(taxonomy_distribution_query).all()
    
    if distribution:
        markdown_rows = [
            "| Common Name | Scientific Name | Order | Linked Images |",
            "|-------------|-----------------|-------|---------------|"
        ]
        
        for (common, scientific, order, image_count) in distribution:
            scientific_display = scientific or "N/A"
            order_display = order or "N/A"
            markdown_rows.append(
                f"| {common} | {scientific_display} | {order_display} | {image_count} |"
            )
        
        metadata["top_20_taxa_by_images"] = dg.MetadataValue.md("\n".join(markdown_rows))
    
    return dg.MaterializeResult(metadata=metadata)
