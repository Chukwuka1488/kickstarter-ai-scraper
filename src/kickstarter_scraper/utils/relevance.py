"""AI-topic relevance scoring for Kickstarter projects."""

from __future__ import annotations

import re

# Weighted keyword groups for AI relevance scoring
AI_KEYWORDS: dict[str, float] = {
    # High signal (0.4 each match)
    r"\bartificial intelligence\b": 0.4,
    r"\bmachine learning\b": 0.4,
    r"\bdeep learning\b": 0.4,
    r"\bneural network": 0.4,
    r"\blarge language model": 0.4,
    r"\bllm\b": 0.4,
    r"\bgenerative ai\b": 0.4,
    r"\bgpt[\s\-]": 0.35,
    r"\bchatgpt\b": 0.35,
    r"\btransformer model": 0.35,
    # Medium signal (0.2 each)
    r"\bcomputer vision\b": 0.2,
    r"\bnatural language processing\b": 0.2,
    r"\bnlp\b": 0.2,
    r"\breinforcement learning\b": 0.2,
    r"\bconvolutional\b": 0.2,
    r"\brecurrent\b": 0.15,
    r"\bgan\b": 0.15,
    r"\bdiffusion model": 0.2,
    r"\btext.to.image\b": 0.2,
    r"\bimage recognition\b": 0.2,
    r"\bspeech recognition\b": 0.15,
    r"\bsentiment analysis\b": 0.15,
    # Lower signal (0.1 each) - contextual
    r"\bai[\s\-]powered\b": 0.15,
    r"\bai[\s\-]driven\b": 0.15,
    r"\bai\b": 0.1,
    r"\bautonomous\b": 0.08,
    r"\bintelligent\b": 0.05,
    r"\bpredictive\b": 0.08,
    r"\brobot": 0.08,
    r"\bautomation\b": 0.05,
    r"\bdata science\b": 0.1,
    r"\balgorithm\b": 0.05,
}

# Negative signals (reduce score) - common false positives
FALSE_POSITIVE_KEYWORDS: dict[str, float] = {
    r"\bai\b.*\ballen iverson\b": -0.3,
    r"\bai\b.*\baisle\b": -0.2,
    r"\bboard game\b": -0.1,
    r"\bcard game\b": -0.1,
    r"\btabletop\b": -0.1,
}


def compute_ai_relevance(name: str, blurb: str = "", description: str = "") -> float:
    """Compute an AI-topic relevance score for a project.

    Combines keyword matching across name (highest weight), blurb, and description.
    Returns a score capped at [0.0, 1.0].

    Args:
        name: Project name/title.
        blurb: Short project tagline.
        description: Full project description.

    Returns:
        Relevance score between 0.0 and 1.0.
    """
    # Weight text sources differently
    texts = [
        (name.lower(), 2.0),       # Name matches worth 2x
        (blurb.lower(), 1.5),      # Blurb matches worth 1.5x
        (description.lower()[:2000], 1.0),  # Description matches worth 1x (truncated)
    ]

    score = 0.0
    matched_keywords = set()

    for text, weight in texts:
        if not text:
            continue
        for pattern, keyword_score in AI_KEYWORDS.items():
            if pattern not in matched_keywords and re.search(pattern, text):
                score += keyword_score * weight
                matched_keywords.add(pattern)

    # Apply negative signals
    full_text = f"{name} {blurb} {description}".lower()
    for pattern, penalty in FALSE_POSITIVE_KEYWORDS.items():
        if re.search(pattern, full_text):
            score += penalty

    # Normalize to 0-1
    return max(0.0, min(1.0, score))
