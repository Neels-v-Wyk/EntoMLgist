# Normalize extracted insect names from comments
import dagster as dg
from sqlmodel import Session, select, func
from EntoMLgist.models.database import Comment
import re

# Common words that are too generic or not actual insect names
INVALID_NAMES = {
    "bug", "bugs", "it", "they", "he", "op", "snot", "mb", "mfs",
    "boots", "buddies shit", "brocoli", "peppermint", "monsteras",
    "hollow knight", "metal mantis", "guten tag", "thasnk", "blindsnake",
    "babosas chiquitas", "squathoppers", "camterpinlr", "beefy thighs",
    "fuzzy butt bugs", "little bugs", "bug bits", "mystery bug",
    "itsacarpetbeetle", "louse-y", "deaths head", "miracle bug",
}

# Word-level synonym replacements (automatically handles compound names)
WORD_SYNONYMS = {
    'roach': 'cockroach',
    'bedbug': 'bed bug',
    'stinkbug': 'stink bug',
    'roly-poly': 'pill bug',
    'rolypoly': 'pill bug',
    'psocid': 'booklouse',
    'cocker': 'cockroach',  # for "cocker roach"
}

# Multi-word phrase replacements (only for actual synonyms/corrections)
PHRASE_REPLACEMENTS = {
    'lady beetle': 'ladybug',
    'book lice': 'booklouse',
    'book louse': 'booklouse',
    'preying mantis': 'praying mantis',  # spelling correction
    'daddy long leg': 'daddy long legs',  # standardize to plural form
}

# Irregular plurals (most common cases)
IRREGULAR_PLURALS = {
    'lice': 'louse',
    'flies': 'fly',
}

# Common suffixes for pluralization
PLURAL_PATTERNS = [
    (r'ches$', 'ch'),      # roaches -> roach (will become cockroach via synonym)
    (r'([^aeiou])ies$', r'\1y'),  # flies -> fly (already in irregular, but for completeness)
    (r'ves$', 'f'),        # leaves -> leaf (not common for insects but good to have)
    (r'([sxz]|ch|sh)es$', r'\1'),  # boxes -> box
    (r'([^s])s$', r'\1'),  # bugs -> bug, beetles -> beetle
]


def singularize(word: str) -> str:
    """Convert plural form to singular using pattern matching."""
    # Check irregular plurals first
    if word in IRREGULAR_PLURALS:
        return IRREGULAR_PLURALS[word]
    
    # Apply plural patterns
    for pattern, replacement in PLURAL_PATTERNS:
        result = re.sub(pattern, replacement, word)
        if result != word:
            return result
    
    return word


def normalize_spacing(text: str) -> str:
    """Normalize spacing in compound names."""
    # Replace multiple spaces/hyphens with single space
    text = re.sub(r'[-\s]+', ' ', text)
    # Remove spaces around hyphens if they remain
    text = re.sub(r'\s*-\s*', '-', text)
    return text.strip()


def replace_word_synonyms(text: str) -> str:
    """Replace word-level synonyms, preserving compound names."""
    words = text.split()
    result = []
    
    for word in words:
        # Clean the word (remove trailing punctuation)
        clean_word = word.strip('.,;:!?')
        
        # Check if it's a synonym
        if clean_word in WORD_SYNONYMS:
            result.append(WORD_SYNONYMS[clean_word])
        else:
            result.append(word)
    
    return ' '.join(result)


def normalize_insect_name(name: str) -> str | None:
    """Normalize an insect name using systematic transformations.
    
    Pipeline:
    1. Lowercase and normalize spacing
    2. Apply phrase-level replacements
    3. Apply word-level synonym replacements
    4. Singularize all words
    5. Validate result
    """
    if not name or not isinstance(name, str):
        return None
    
    normalized = name.strip().lower()
    normalized = normalize_spacing(normalized)
    
    if normalized in INVALID_NAMES:
        return None
    
    for phrase, replacement in PHRASE_REPLACEMENTS.items():
        normalized = normalized.replace(phrase, replacement)
    
    normalized = replace_word_synonyms(normalized)
    
    words = normalized.split()
    singular_words = [singularize(word) for word in words]
    normalized = ' '.join(singular_words)
    
    normalized = normalize_spacing(normalized)
    
    if len(normalized) <= 2 or normalized in INVALID_NAMES:
        return None
    
    return normalized


@dg.asset(required_resource_keys={"db_session"}, deps=["extract_insect_names_from_comments"])
def normalize_insect_names(context: dg.AssetExecutionContext):
    """Normalize extracted insect names to handle capitalization, pluralization, and invalid names."""
    session: Session = context.resources.db_session
    
    # Get all comments with extracted names
    statement = select(Comment).where(Comment.extracted_name.isnot(None))
    comments = session.exec(statement).all()
    
    normalized_count = 0
    invalidated_count = 0
    
    for comment in comments:
        original_name = comment.extracted_name
        normalized_name = normalize_insect_name(original_name)
        
        if normalized_name is None:
            # Invalid/generic name - clear it
            context.log.info(f"Clearing invalid name '{original_name}' from comment {comment.comment_id}")
            comment.extracted_name = None
            comment.extracted_name_confidence = None
            invalidated_count += 1
        elif normalized_name != original_name:
            # Name was changed during normalization (case, pluralization, synonyms, etc.)
            context.log.info(f"Normalized '{original_name}' -> '{normalized_name}' in comment {comment.comment_id}")
            comment.extracted_name = normalized_name
            normalized_count += 1
        
        session.add(comment)
    
    session.commit()
    
    context.log.info(f"Normalized {normalized_count} names, invalidated {invalidated_count} generic/invalid names")
    
    # Query extraction statistics after normalization
    name_stats_query = (
        select(
            Comment.extracted_name,
            func.count(Comment.extracted_name).label('count'),
            func.avg(Comment.extracted_name_confidence).label('avg_conf'),
            func.min(Comment.extracted_name_confidence).label('min_conf'),
            func.max(Comment.extracted_name_confidence).label('max_conf')
        )
        .where(Comment.extracted_name.isnot(None))
        .group_by(Comment.extracted_name)
        .order_by(func.count(Comment.extracted_name).desc())
        .limit(20)  # Top 20 most common names
    )
    name_stats = session.exec(name_stats_query).all()
    
    # Overall statistics
    total_extracted = session.exec(
        select(func.count()).select_from(Comment).where(Comment.extracted_name.isnot(None))
    ).one()
    
    avg_overall_conf = session.exec(
        select(func.avg(Comment.extracted_name_confidence))
        .where(Comment.extracted_name_confidence.isnot(None))
    ).one()
    
    # Build metadata for Dagster UI
    metadata = {
        "normalized_count": dg.MetadataValue.int(normalized_count),
        "invalidated_count": dg.MetadataValue.int(invalidated_count),
        "total_processed": dg.MetadataValue.int(len(comments)),
        "total_extracted_names": dg.MetadataValue.int(total_extracted),
        "unique_insect_names": dg.MetadataValue.int(len(name_stats)),
    }
    
    if avg_overall_conf is not None:
        metadata["avg_confidence"] = dg.MetadataValue.float(round(avg_overall_conf, 3))
    
    # Create markdown table of top extracted names
    if name_stats:
        markdown_rows = ["| Rank | Name | Count | Avg Conf | Min Conf | Max Conf |",
                        "|------|------|-------|----------|----------|----------|"]
        
        for idx, (name, count, avg_conf, min_conf, max_conf) in enumerate(name_stats, 1):
            avg_str = f"{avg_conf:.3f}" if avg_conf is not None else "N/A"
            min_str = f"{min_conf:.3f}" if min_conf is not None else "N/A"
            max_str = f"{max_conf:.3f}" if max_conf is not None else "N/A"
            markdown_rows.append(f"| {idx} | {name} | {count} | {avg_str} | {min_str} | {max_str} |")
        
        metadata["top_20_extracted_names"] = dg.MetadataValue.md("\n".join(markdown_rows))
    
    return dg.MaterializeResult(metadata=metadata)
