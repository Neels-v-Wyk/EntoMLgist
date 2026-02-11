"""Example queries for filtering training data using label quality scores.

The label_quality_score combines:
- GLiNER extraction confidence (0-1): How confident the model is about the extracted name
- GBIF match confidence (0-100): How confident GBIF is about the taxonomic match

Final score = extraction_confidence * (gbif_confidence / 100)

Recommended thresholds:
- 0.8+: High quality - use for validation set
- 0.6-0.8: Medium quality - good for training
- 0.4-0.6: Low quality - review manually or exclude
- <0.4: Very low quality - likely noise, exclude
"""

from sqlmodel import Session, select, func
from EntoMLgist.models.database import ImageUrl, TaxonomicName, ImageTaxonomyLink
from EntoMLgist.database_config import engine


def get_high_quality_training_data(session: Session, min_quality: float = 0.7):
    """Get high-quality labeled images for training.
    
    Args:
        session: Database session
        min_quality: Minimum label quality score (0-1)
        
    Returns:
        List of tuples: (image_path, common_name, scientific_name, order, family, quality_score)
    """
    query = (
        select(
            ImageUrl.local_path,
            TaxonomicName.common_name,
            TaxonomicName.scientific_name,
            TaxonomicName.order,
            TaxonomicName.family,
            ImageTaxonomyLink.label_quality_score
        )
        .join(ImageTaxonomyLink, ImageUrl.image_id == ImageTaxonomyLink.image_id)
        .join(TaxonomicName, ImageTaxonomyLink.common_name == TaxonomicName.common_name)
        .where(ImageUrl.downloaded == 1)
        .where(TaxonomicName.is_insect == True)
        .where(ImageTaxonomyLink.label_quality_score >= min_quality)
        .order_by(ImageTaxonomyLink.label_quality_score.desc())
    )
    
    return session.exec(query).all()


def get_stratified_sample_by_order(session: Session, min_quality: float = 0.6, samples_per_order: int = 100):
    """Get stratified sample of training data balanced across insect orders.
    
    Useful for preventing class imbalance in training data.
    """
    # First get all orders
    orders_query = (
        select(TaxonomicName.order)
        .join(ImageTaxonomyLink)
        .where(TaxonomicName.is_insect == True)
        .where(ImageTaxonomyLink.label_quality_score >= min_quality)
        .distinct()
    )
    orders = session.exec(orders_query).all()
    
    results = []
    for order in orders:
        if order is None:
            continue
            
        query = (
            select(
                ImageUrl.local_path,
                TaxonomicName.common_name,
                TaxonomicName.scientific_name,
                TaxonomicName.order,
                TaxonomicName.family,
                ImageTaxonomyLink.label_quality_score
            )
            .join(ImageTaxonomyLink, ImageUrl.image_id == ImageTaxonomyLink.image_id)
            .join(TaxonomicName, ImageTaxonomyLink.common_name == TaxonomicName.common_name)
            .where(ImageUrl.downloaded == 1)
            .where(TaxonomicName.order == order)
            .where(ImageTaxonomyLink.label_quality_score >= min_quality)
            .order_by(ImageTaxonomyLink.label_quality_score.desc())
            .limit(samples_per_order)
        )
        
        results.extend(session.exec(query).all())
    
    return results


def analyze_quality_distribution(session: Session):
    """Analyze the distribution of label quality scores."""
    # Overall statistics
    stats = session.exec(
        select(
            func.count(ImageTaxonomyLink.image_id).label('total'),
            func.avg(ImageTaxonomyLink.label_quality_score).label('avg'),
            func.min(ImageTaxonomyLink.label_quality_score).label('min'),
            func.max(ImageTaxonomyLink.label_quality_score).label('max')
        )
        .where(ImageTaxonomyLink.label_quality_score.isnot(None))
    ).one()
    
    print(f"Total labeled images: {stats[0]}")
    print(f"Average quality: {stats[1]:.3f}")
    print(f"Min quality: {stats[2]:.3f}")
    print(f"Max quality: {stats[3]:.3f}")
    print()
    
    # Quality buckets
    buckets = [
        (0.8, 1.0, "Excellent"),
        (0.6, 0.8, "Good"),
        (0.4, 0.6, "Fair"),
        (0.0, 0.4, "Poor")
    ]
    
    print("Quality Distribution:")
    print("-" * 50)
    for min_q, max_q, label in buckets:
        count = session.exec(
            select(func.count(ImageTaxonomyLink.image_id))
            .where(ImageTaxonomyLink.label_quality_score >= min_q)
            .where(ImageTaxonomyLink.label_quality_score < max_q)
        ).one()
        percentage = (count / stats[0] * 100) if stats[0] > 0 else 0
        print(f"{label:10s} ({min_q:.1f}-{max_q:.1f}): {count:5d} ({percentage:5.1f}%)")


def get_taxonomy_distribution_by_quality(session: Session, min_quality: float = 0.7):
    """Show how many images per order meet the quality threshold."""
    query = (
        select(
            TaxonomicName.order,
            func.count(ImageTaxonomyLink.image_id.distinct()).label('image_count'),
            func.avg(ImageTaxonomyLink.label_quality_score).label('avg_quality')
        )
        .join(ImageTaxonomyLink, ImageTaxonomyLink.common_name == TaxonomicName.common_name)
        .join(ImageUrl, ImageUrl.image_id == ImageTaxonomyLink.image_id)
        .where(ImageUrl.downloaded == 1)
        .where(TaxonomicName.is_insect == True)
        .where(ImageTaxonomyLink.label_quality_score >= min_quality)
        .group_by(TaxonomicName.order)
        .order_by(func.count(ImageTaxonomyLink.image_id.distinct()).desc())
    )
    
    results = session.exec(query).all()
    
    print(f"\nTaxonomy Distribution (quality >= {min_quality}):")
    print("-" * 60)
    print(f"{'Order':<25s} {'Images':>10s} {'Avg Quality':>15s}")
    print("-" * 60)
    
    for order, count, avg_quality in results:
        order_name = order or "Unknown"
        print(f"{order_name:<25s} {count:>10d} {avg_quality:>15.3f}")
    
    return results


def export_training_metadata(session: Session, output_path: str = "training_data.json", min_quality: float = 0.7):
    """Export training metadata to JSON file.
    
    Creates a JSON file with image paths and labels filtered by quality.
    Compatible with most deep learning frameworks.
    """
    import json
    
    data = get_high_quality_training_data(session, min_quality)
    
    training_samples = []
    for local_path, common_name, scientific_name, order, family, quality in data:
        training_samples.append({
            "image_path": local_path,
            "label": {
                "common_name": common_name,
                "scientific_name": scientific_name,
                "order": order,
                "family": family
            },
            "quality_score": quality,
            "extraction_confidence": None,  # Would need to join to get this
            "gbif_confidence": None  # Would need to join to get this
        })
    
    with open(output_path, 'w') as f:
        json.dump({
            "min_quality_threshold": min_quality,
            "total_samples": len(training_samples),
            "samples": training_samples
        }, f, indent=2)
    
    print(f"Exported {len(training_samples)} training samples to {output_path}")


if __name__ == "__main__":
    # Example usage
    with Session(engine) as session:
        print("=" * 60)
        print("LABEL QUALITY ANALYSIS")
        print("=" * 60)
        print()
        
        # Analyze quality distribution
        analyze_quality_distribution(session)
        print()
        
        # Show taxonomy distribution for high-quality labels
        get_taxonomy_distribution_by_quality(session, min_quality=0.7)
        print()
        
        # Export high-quality training data
        # export_training_metadata(session, "training_data.json", min_quality=0.7)
