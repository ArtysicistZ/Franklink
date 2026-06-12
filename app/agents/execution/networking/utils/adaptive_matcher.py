"""Adaptive matcher for networking.

Uses structured complementary matching (supply-demand skill intersection) and
cross-user knowledge graph traversal for candidate retrieval, then LLM-based
selection to find the best networking match that satisfies the user's demand
while ensuring mutual benefit.
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from app.database.client import DatabaseClient
from app.integrations.azure_openai_client import AzureOpenAIClient

logger = logging.getLogger(__name__)


# LLM Prompts for match selection
MATCH_SELECTION_SYSTEM_PROMPT = """You are a networking match analyzer for Franklink, a professional networking platform for students and early-career professionals.

Your task: Select the SINGLE BEST match from a list of candidates for the initiator's networking request.

## CRITICAL: Be SPECIFIC and CONCRETE

Franklink's core differentiation is that every intro ACTUALLY MATTERS. Generic explanations like "they share similar interests" or "both interested in startups" are USELESS. Users need to understand the SPECIFIC, CONCRETE reasons why this connection makes sense.

WRONG (too generic - users won't feel the value):
- "Both are interested in technology and startups"
- "They have complementary skills"
- "Similar career interests in finance"

RIGHT (specific and compelling - users understand the unique fit):
- "Beatrice is actively building a fintech startup and needs someone with Eric's ML experience for their recommendation engine"
- "Jimmy just finished a trading systems project at D.E. Shaw - exactly the HFT background Eric wants to learn from"
- "Steven is recruiting for his hackathon team and specifically needs frontend skills, which matches Eric's React portfolio"

## Evaluation Criteria

1. **Demand Satisfaction (35%)**: How SPECIFICALLY can this candidate help?
   - What CONCRETE skill, experience, project, or knowledge do they have?
   - Reference SPECIFIC details: company names, project names, course names, technologies
   - A "hackathon teammate" match should mention their specific tech stack or past project
   - A "mentor" match should mention their specific role, company, or experience

2. **Mutual Benefit (35%)**: What SPECIFIC motivation would the candidate have?
   - What does the INITIATOR offer that the CANDIDATE specifically needs?
   - Reference the candidate's stated demand or latent needs with specifics
   - "Eric's Franklink app experience could help with Beatrice's user engagement problem"
   - NOT "they could help each other" (too vague)

3. **Holistic Compatibility (20%)**: What SPECIFIC personality or style alignment exists?
   - Reference specific traits, working styles, or relationship preferences
   - "Both prefer hands-on collaboration over formal mentorship"
   - NOT "good personality fit" (meaningless)

4. **Context Compatibility (10%)**: What PRACTICAL factors make this work?
   - Same campus, same class year, overlapping schedules, shared events
   - "Both attending the Penn Blockchain hackathon this weekend"

## Red Flags (Reduce confidence significantly)
- Candidate has no clear value to offer AND initiator has nothing the candidate wants
- Candidate's demand directly conflicts with being available for this type of connection
- Obvious mismatch in career stage that makes exchange difficult
- Candidate's ideal relationship types don't include what initiator is looking for

## Output Format
Return ONLY valid JSON (no markdown, no explanation):
{
    "selected_user_id": "<uuid of best match>",
    "confidence": <0.0-1.0>,
    "rationale": {
        "demand_satisfaction": "<1-2 sentences with SPECIFIC details: names, companies, projects, skills>",
        "mutual_benefit": "<1-2 sentences explaining what SPECIFICALLY motivates the candidate>",
        "context_fit": "<1 sentence: specific practical factors>"
    },
    "match_summary": "<1 sentence with the SINGLE most compelling reason for this match>",
    "concern": "<any concern about this match, or null if none>"
}"""


GRAPH_REASONING_SYSTEM_PROMPT = """You are a graph-aware networking match analyzer for Franklink, a professional networking platform for students and early-career professionals.

Your task: Select the BEST match from a list of candidates for the initiator's networking request. You receive BOTH candidate profiles AND a knowledge graph showing how each candidate connects to the initiator through shared experiences, skills, and contexts.

## Graph Connection Tiers (Weighted by Strength)

Tier 1 — DIRECT DEMAND FULFILLMENT (Strongest signal, 40%):
  The candidate offers a specific skill or resource the initiator explicitly needs, verified through the knowledge graph. This is the most valuable connection type.

Tier 2 — SHARED ACTIVE CONTEXT (Strong signal, 30%):
  Both people are actively involved in the same project, club, course, or event. This provides a natural "warm intro" angle — they already have something concrete to talk about.

Tier 3 — SHARED PASSIVE CONTEXT (Moderate signal, 15%):
  Both are at the same university, in the same city, or interested in the same broad domain. Provides common ground but not a specific conversation starter.

Tier 4 — PARALLEL PATHS (Weak signal, 15%):
  Both seek the same career role, share the same skill, or follow similar interests. Indicates alignment but not direct complementarity.

## CRITICAL: Multi-Path Connections

When a candidate connects to the initiator through MULTIPLE tiers or multiple nodes within the same tier, this is exponentially more valuable than a single connection. A candidate with 3 connection paths is much stronger than one with only 1 — even if that single path is Tier 1.

## CRITICAL: Be SPECIFIC and CONCRETE

Generic explanations are USELESS. Reference specific graph paths, node names, and connection types.

WRONG: "They share similar interests and could help each other"
RIGHT: "Beatrice offers 'ml-engineering' which Eric needs (Tier 1), and both work on project 'trading-bot' (Tier 2) — she could directly help with the recommendation engine while they already have a shared project to bond over"

## Evaluation Criteria

1. **Graph-Verified Demand Satisfaction (40%)**: Does the knowledge graph CONFIRM this candidate can help?
   - Tier 1 signals are definitive proof of relevance
   - Cite the specific graph path: "[Candidate] offers [skill] which you need"

2. **Natural Introduction Angle (30%)**: Is there a "warm intro" path?
   - Tier 2 shared active context provides the best conversation starters
   - "You're both in [club/course/event] — that's a natural way to connect"

3. **Mutual Benefit (20%)**: What motivates the CANDIDATE to engage?
   - Check if the initiator offers skills the candidate needs (reverse Tier 1)
   - Check the candidate's stated demand vs initiator's value proposition

4. **Holistic Compatibility (10%)**: Career stage, personality, relationship type alignment

## Red Flags
- Candidate has ZERO graph connections AND weak profile match — low confidence
- Candidate's demand directly conflicts with what the initiator is looking for
- Graph shows candidate is in a completely different domain with no bridge nodes

## Output Format
Return ONLY valid JSON (no markdown, no explanation):
{
    "selected_user_id": "<uuid of best match>",
    "confidence": <0.0-1.0>,
    "rationale": {
        "demand_satisfaction": "<1-2 sentences citing SPECIFIC graph paths and profile details>",
        "mutual_benefit": "<1-2 sentences explaining what SPECIFICALLY motivates the candidate>",
        "introduction_angle": "<1 sentence: the best conversation starter based on shared context>",
        "context_fit": "<1 sentence: practical factors>"
    },
    "graph_paths_cited": ["<tier>: <node_name>", ...],
    "match_summary": "<1 sentence with the SINGLE most compelling reason, citing a graph path>",
    "concern": "<any concern about this match, or null if none>"
}"""


GRAPH_REASONING_MULTI_SELECT_SYSTEM_PROMPT = """You are a graph-aware networking match analyzer for Franklink, a professional networking platform for students and early-career professionals.

Your task: Select the BEST N matches from a list of candidates for a GROUP connection. You receive BOTH candidate profiles AND a knowledge graph showing how each candidate connects to the initiator through shared experiences, skills, and contexts.

## Graph Connection Tiers (Weighted by Strength)

Tier 1 — DIRECT DEMAND FULFILLMENT (40%): Candidate offers a skill the initiator needs, verified via graph.
Tier 2 — SHARED ACTIVE CONTEXT (30%): Same project, club, course, or event. Natural warm intro angle.
Tier 3 — SHARED PASSIVE CONTEXT (15%): Same university, city, or domain. Common ground.
Tier 4 — PARALLEL PATHS (15%): Same career role sought, similar skills. Alignment but not complementarity.

Multi-path connections (multiple tiers or nodes) are exponentially more valuable than single connections.

## Group Composition Strategy

When selecting multiple matches for a group:
1. **Diversity of value**: Each selected candidate should bring DIFFERENT strengths
2. **Complementary coverage**: The group as a whole should cover the initiator's demand more completely than any single person
3. **Shared context overlap**: Prefer candidates who share context with EACH OTHER (not just the initiator) — this makes the group gel naturally
4. **No redundancy**: Avoid selecting two candidates who offer the exact same skill/value

## CRITICAL: Be SPECIFIC and CONCRETE
Reference specific graph paths, node names, and connection types. Generic explanations are USELESS.

## Output Format
Return ONLY valid JSON (no markdown, no explanation):
{
    "selections": [
        {
            "selected_user_id": "<uuid>",
            "confidence": <0.0-1.0>,
            "rationale": {
                "demand_satisfaction": "<1-2 sentences citing SPECIFIC graph paths>",
                "mutual_benefit": "<1-2 sentences>",
                "introduction_angle": "<1 sentence>",
                "context_fit": "<1 sentence>"
            },
            "graph_paths_cited": ["<tier>: <node_name>", ...],
            "match_summary": "<1 sentence>",
            "concern": "<concern or null>",
            "group_role": "<what unique value this person adds to the group>"
        }
    ],
    "group_synergy": "<1-2 sentences explaining why these specific people work well TOGETHER>"
}"""


# Prompt to extract structured skills from a user's networking demand
DEMAND_SKILL_EXTRACTION_PROMPT = """Extract skills from this networking request.

Request: {demand_text}

Return ONLY valid JSON:
{{
    "seeking_skills": ["skill-1", "skill-2"],
    "seeking_relationship_types": ["mentor", "collaborator"]
}}

Skill taxonomy (use these normalized, lowercase, hyphenated terms):
- Skills: marketing, growth-marketing, sales, fundraising, engineering, frontend, backend, mobile-dev,
  ml-engineering, data-science, product-management, design, ux-design, graphic-design, finance,
  accounting, legal, operations, business-development, content-creation, social-media, research,
  data-analysis, cloud-infrastructure, devops, cybersecurity, blockchain, ai-ml, nlp, robotics,
  hardware, biotech, consulting, project-management, public-speaking, writing, mentorship,
  trading, quantitative-finance, supply-chain, hr, recruiting, customer-success, community-management,
  fashion, beauty, retail, e-commerce, healthcare, real-estate, media, entertainment, music,
  sports, gaming, education, non-profit, government, law, architecture, manufacturing
- Relationship types: mentor, mentee, co-founder, collaborator, study-partner, accountability-partner,
  advisor, investor, hiring-manager, teammate, peer, industry-contact, domain-expert, technical-advisor

Examples:
- "mentor in AI field" -> {{"seeking_skills": ["ai-ml", "ml-engineering"], "seeking_relationship_types": ["mentor"]}}
- "someone in beauty and fashion" -> {{"seeking_skills": ["fashion", "beauty", "retail"], "seeking_relationship_types": ["industry-contact", "peer"]}}
- "co-founder for fintech startup" -> {{"seeking_skills": ["finance", "fintech", "engineering"], "seeking_relationship_types": ["co-founder"]}}
- "machine learning mentor who works at a tech company" -> {{"seeking_skills": ["ml-engineering", "ai-ml", "data-science"], "seeking_relationship_types": ["mentor"]}}

IMPORTANT: Be generous with skill extraction. If the user mentions a domain or industry, include relevant skills.
For vague requests, infer the most likely skills based on context."""


@dataclass
class GraphNarrative:
    """Pre-computed graph narrative for a candidate's connections to the initiator."""
    user_id: str
    tier1_signals: List[str] = field(default_factory=list)
    tier2_signals: List[str] = field(default_factory=list)
    tier3_signals: List[str] = field(default_factory=list)
    tier4_signals: List[str] = field(default_factory=list)
    shared_node_names: List[str] = field(default_factory=list)
    narrative_text: str = "No graph connections found."


@dataclass
class CandidateProfile:
    """Rich candidate profile for LLM evaluation."""
    user_id: str
    name: str
    phone_number: str
    university: Optional[str] = None
    major: Optional[str] = None
    year: Optional[str] = None
    location: Optional[str] = None
    career_interests: List[str] = field(default_factory=list)
    all_demand: Optional[str] = None
    all_value: Optional[str] = None
    context_summary: Optional[str] = None
    needs: List[Any] = field(default_factory=list)
    linkedin_data: Optional[Dict[str, Any]] = None

    # Match metadata
    match_sources: Set[str] = field(default_factory=set)
    similarity_scores: Dict[str, float] = field(default_factory=dict)

    # Zep knowledge graph enrichment
    zep_facts: List[str] = field(default_factory=list)

    # Holistic profile data (from user_profiles table)
    holistic_summary: Optional[str] = None
    latent_needs: List[str] = field(default_factory=list)
    ideal_relationship_types: List[str] = field(default_factory=list)
    career_stage: Optional[str] = None
    relationship_strengths: Optional[str] = None

    def get_background_context(self) -> str:
        """Extract relevant background context from personal_facts and linkedin."""
        context_parts = []

        # From linkedin_data
        if self.linkedin_data:
            if headline := self.linkedin_data.get("headline"):
                context_parts.append(f"LinkedIn: {headline}")
            if experiences := self.linkedin_data.get("experiences", []):
                recent = experiences[0] if experiences else {}
                if title := recent.get("title"):
                    company = recent.get("company", "")
                    context_parts.append(f"Current/Recent: {title} at {company}")
            if skills := self.linkedin_data.get("skills", []):
                context_parts.append(f"Skills: {', '.join(skills[:5])}")

        # From needs
        if self.needs:
            needs_str = ", ".join(str(n) for n in self.needs[:3])
            context_parts.append(f"Career needs: {needs_str}")

        return "\n".join(context_parts) if context_parts else "No additional context"

    def to_llm_format(self, index: int) -> str:
        """Format candidate for LLM prompt."""
        careers = ", ".join(self.career_interests) if self.career_interests else "Not specified"
        sources = ", ".join(self.match_sources) if self.match_sources else "unknown"
        best_similarity = max(self.similarity_scores.values()) if self.similarity_scores else 0.0

        # Build Zep insights section if available
        zep_insights = ""
        if self.zep_facts:
            zep_insights = "\n\n**Email/Context Insights:**\n" + "\n".join(
                f"- {fact}" for fact in self.zep_facts[:3]
            )

        # Build holistic profile section if available
        holistic_section = ""
        if self.holistic_summary:
            latent_needs_str = ", ".join(self.latent_needs) if self.latent_needs else "Not analyzed"
            relationship_types_str = ", ".join(self.ideal_relationship_types) if self.ideal_relationship_types else "Not analyzed"
            holistic_section = f"""

**Holistic Profile (AI-synthesized understanding):**
{self.holistic_summary}

- Latent Needs: {latent_needs_str}
- Ideal Relationship Types: {relationship_types_str}
- Career Stage: {self.career_stage or 'Unknown'}
- Relationship Strengths: {self.relationship_strengths or 'Not analyzed'}"""

        return f"""
### Candidate {index}: {self.name or 'Unknown'}
- User ID: {self.user_id}
- Match Sources: {sources} (best similarity: {best_similarity:.2f})
- University: {self.university or 'Not specified'} | Major: {self.major or 'Not specified'} | Year: {self.year or 'N/A'}
- Location: {self.location or 'Not specified'}
- Career Interests: {careers}
- Context Summary: {self.context_summary or 'Not available'}

**What they're looking for (DEMAND):**
{self.all_demand or 'Not specified'}

**What they can offer (VALUE):**
{self.all_value or 'Not specified'}

**Background/Context:**
{self.get_background_context()}{zep_insights}{holistic_section}
"""


@dataclass
class AdaptiveMatchResult:
    """Result from adaptive matching."""
    success: bool = False
    error_message: Optional[str] = None

    # Target user info
    target_user_id: Optional[str] = None
    target_name: Optional[str] = None
    target_phone: Optional[str] = None

    # Match quality
    match_confidence: float = 0.0
    match_score: float = 0.0  # Alias for match_confidence
    demand_satisfaction: Optional[str] = None
    mutual_benefit: Optional[str] = None
    match_summary: Optional[str] = None
    concern: Optional[str] = None
    matching_reasons: List[str] = field(default_factory=list)
    llm_introduction: Optional[str] = None
    llm_concern: Optional[str] = None


class AdaptiveMatcher:
    """
    Adaptive matcher for networking connections.

    Uses two parallel candidate sources:
    1. Structured complementary matching (supply-demand set intersection)
    2. Cross-user knowledge graph (shared orgs, domains, skill paths)

    Candidates are merged, deduplicated, enriched with Zep facts and
    holistic profiles, then evaluated by LLM for final selection.
    """

    CANDIDATE_POOL_SIZE = 8  # Number of candidates to pass to LLM
    COMPLEMENTARY_MATCH_COUNT = 25  # Max candidates from structured skill matching
    GRAPH_MATCH_COUNT = 15  # Max candidates from knowledge graph matching

    def __init__(
        self,
        db: Optional[DatabaseClient] = None,
        openai: Optional[AzureOpenAIClient] = None,
    ):
        """Initialize the matcher.

        Args:
            db: Database client (creates one if not provided)
            openai: OpenAI client for LLM selection (creates one if not provided)
        """
        self.db = db or DatabaseClient()
        self.openai = openai or AzureOpenAIClient()

    async def _extract_skills_from_demand(
        self,
        demand_text: str,
    ) -> Dict[str, List[str]]:
        """Extract structured skills from a user's networking demand.

        Uses LLM to parse the demand text and extract normalized skill arrays
        that can be used for complementary matching.

        Args:
            demand_text: The user's networking request (e.g., "find me a mentor in AI field")

        Returns:
            Dict with 'seeking_skills' and 'seeking_relationship_types' arrays
        """
        try:
            prompt = DEMAND_SKILL_EXTRACTION_PROMPT.format(demand_text=demand_text)

            response = await self.openai.generate_response(
                system_prompt=prompt,
                user_prompt=f"Extract skills from: {demand_text}",
                model="gpt-4o-mini",
                temperature=0.0,
                trace_label="demand_skill_extraction",
            )

            # Parse JSON response
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            result = json.loads(cleaned)

            # Normalize skills (lowercase, hyphenated)
            seeking_skills = []
            for skill in result.get("seeking_skills", []):
                if isinstance(skill, str):
                    normalized = skill.strip().lower().replace(" ", "-").replace("_", "-")
                    if normalized:
                        seeking_skills.append(normalized)

            seeking_rel_types = []
            for rel_type in result.get("seeking_relationship_types", []):
                if isinstance(rel_type, str):
                    normalized = rel_type.strip().lower().replace(" ", "-").replace("_", "-")
                    if normalized:
                        seeking_rel_types.append(normalized)

            logger.info(
                f"[ADAPTIVE_MATCHER] Extracted skills from demand:\n"
                f"  - Demand: {demand_text[:80]}...\n"
                f"  - seeking_skills: {seeking_skills}\n"
                f"  - seeking_relationship_types: {seeking_rel_types}"
            )

            return {
                "seeking_skills": seeking_skills,
                "seeking_relationship_types": seeking_rel_types,
            }

        except json.JSONDecodeError as e:
            logger.warning(
                f"[ADAPTIVE_MATCHER] Failed to parse skill extraction response: {e}\n"
                f"  - Demand: {demand_text[:80]}...\n"
                f"  - Falling back to empty skills"
            )
            return {"seeking_skills": [], "seeking_relationship_types": []}
        except Exception as e:
            logger.warning(
                f"[ADAPTIVE_MATCHER] Skill extraction failed: {e}\n"
                f"  - Demand: {demand_text[:80]}...\n"
                f"  - Falling back to empty skills"
            )
            return {"seeking_skills": [], "seeking_relationship_types": []}

    async def find_best_match(
        self,
        user_id: str,
        user_profile: Dict[str, Any],
        excluded_user_ids: Optional[List[str]] = None,
        override_demand: Optional[str] = None,
        override_value: Optional[str] = None,
    ) -> AdaptiveMatchResult:
        """Find the best match using adaptive multi-signal approach.

        Args:
            user_id: The initiator's user ID
            user_profile: The initiator's profile data
            excluded_user_ids: User IDs to exclude from matching
            override_demand: Override the user's demand for this search
            override_value: Override the user's value for this search

        Returns:
            AdaptiveMatchResult with match details or error
        """
        try:
            excluded = excluded_user_ids or []

            # DEBUG: Log full profile state for debugging matching issues
            logger.info(
                f"[ADAPTIVE_MATCHER] ========== FIND_BEST_MATCH START ==========\n"
                f"  User ID: {user_id}\n"
                f"  User Name: {user_profile.get('name', 'Unknown')}\n"
                f"  Override Demand: {override_demand or '(none)'}\n"
                f"  Override Value: {override_value or '(none)'}\n"
                f"  Excluded Users: {len(excluded)} users"
            )
            logger.info(
                f"[ADAPTIVE_MATCHER] Profile skills state:\n"
                f"  - seeking_skills: {user_profile.get('seeking_skills') or '(empty/missing)'}\n"
                f"  - offering_skills: {user_profile.get('offering_skills') or '(empty/missing)'}\n"
                f"  - latest_demand: {(user_profile.get('latest_demand') or '(none)')[:100]}\n"
                f"  - all_demand: {(user_profile.get('all_demand') or '(none)')[:100]}"
            )

            # Get the initiator's demand and value
            demand_text = (
                override_demand or
                user_profile.get("latest_demand") or
                user_profile.get("all_demand")
            )
            value_text = override_value or user_profile.get("all_value")

            if not demand_text:
                logger.warning(
                    f"[ADAPTIVE_MATCHER] No demand text available for user {user_id}. "
                    "Cannot proceed with matching."
                )
                return AdaptiveMatchResult(
                    success=False,
                    error_message="No demand specified. What kind of help are you looking for?",
                )

            logger.info(f"[ADAPTIVE_MATCHER] Finding match for user {user_id}")
            logger.info(f"[ADAPTIVE_MATCHER] Demand: {demand_text[:100]}...")

            # Phase 1: Generate candidate pool via structured complementary matching
            logger.info("[ADAPTIVE_MATCHER] Phase 1: Generating candidate pool...")
            candidates = await self._generate_candidate_pool(
                user_id=user_id,
                user_profile=user_profile,
                excluded_user_ids=excluded,
                override_demand=override_demand,
            )

            if not candidates:
                logger.warning(
                    f"[ADAPTIVE_MATCHER] ZERO candidates generated for user {user_id}.\n"
                    f"  This is the root cause of 'network came up empty' responses.\n"
                    f"  Check if user has seeking_skills/offering_skills populated.\n"
                    f"  Check if profile synthesis has run for this user."
                )
                return AdaptiveMatchResult(
                    success=False,
                    error_message=(
                        "No suitable matches found at this time. "
                        "Try being more specific about what you're looking for."
                    ),
                )

            logger.info(
                f"[ADAPTIVE_MATCHER] Generated {len(candidates)} candidates, "
                f"passing top {min(len(candidates), self.CANDIDATE_POOL_SIZE)} to LLM"
            )

            # Phase 1.5a: Enrich candidates with Zep knowledge graph facts
            candidates = await self._enrich_candidates_with_zep(
                candidates=candidates,
                initiator_demand=demand_text,
            )

            # Phase 1.5b: Enrich candidates with holistic profile data
            candidates = await self._enrich_candidates_with_profiles(
                candidates=candidates,
            )

            # Phase 2: LLM selection (graph-first reasoning)
            selection = await self._select_match(
                initiator_profile=user_profile,
                initiator_user_id=user_id,
                demand_text=demand_text,
                value_text=value_text,
                candidates=candidates[:self.CANDIDATE_POOL_SIZE],
            )

            if not selection:
                return AdaptiveMatchResult(
                    success=False,
                    error_message=(
                        "Could not determine a graph-grounded match. "
                        "Please sync graph context and try again."
                    ),
                )

            # Find the selected candidate
            selected_id = selection.get("selected_user_id")
            selected = next(
                (c for c in candidates if c.user_id == selected_id),
                None
            )

            if not selected:
                logger.error(f"[ADAPTIVE_MATCHER] LLM selected unknown user: {selected_id}")
                return AdaptiveMatchResult(
                    success=False,
                    error_message="Match selection error. Please try again.",
                )

            # Build result
            rationale = selection.get("rationale", {})
            matching_reasons = self._build_matching_reasons(selected, rationale)

            return AdaptiveMatchResult(
                success=True,
                target_user_id=selected.user_id,
                target_name=selected.name,
                target_phone=selected.phone_number,
                match_confidence=selection.get("confidence", 0.0),
                match_score=selection.get("confidence", 0.0),
                demand_satisfaction=rationale.get("demand_satisfaction"),
                mutual_benefit=rationale.get("mutual_benefit"),
                match_summary=selection.get("match_summary"),
                concern=selection.get("concern"),
                matching_reasons=matching_reasons,
                llm_introduction=selection.get("match_summary"),
                llm_concern=selection.get("concern"),
            )

        except Exception as e:
            logger.error(f"[ADAPTIVE_MATCHER] find_best_match failed: {e}", exc_info=True)
            return AdaptiveMatchResult(
                success=False,
                error_message=f"Match search failed: {str(e)}",
            )

    async def _generate_candidate_pool(
        self,
        user_id: str,
        user_profile: Dict[str, Any],
        excluded_user_ids: List[str],
        override_demand: Optional[str] = None,
    ) -> List[CandidateProfile]:
        """Generate candidates from multiple sources in parallel.

        Runs two candidate sources concurrently:
        1. Complementary matching: supply-demand skill set intersection
        2. Graph matching: shared orgs, domains, and skill paths

        Results are merged and deduplicated by user_id. Users appearing
        in multiple sources get combined match_sources and similarity_scores.

        Args:
            user_id: Initiator's user ID
            user_profile: Initiator's profile
            excluded_user_ids: Users to exclude
            override_demand: Explicit demand from user (used to derive seeking_skills)

        Returns:
            List of CandidateProfile objects sorted by source count then score
        """
        from app.config import settings

        tasks = []

        # Source 1: Complementary matching
        if getattr(settings, "complementary_matching_enabled", True):
            tasks.append(
                self._get_complementary_candidates(
                    user_id, user_profile, excluded_user_ids, override_demand
                )
            )

        # Source 2: Graph matching
        if getattr(settings, "graph_matching_enabled", False):
            tasks.append(
                self._get_graph_candidates(user_id, excluded_user_ids)
            )

        if not tasks:
            logger.warning("[ADAPTIVE_MATCHER] All candidate sources disabled")
            return []

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge and deduplicate across sources
        merged = self._merge_candidate_pools(results)

        logger.info(f"[ADAPTIVE_MATCHER] Candidate pool: {len(merged)} candidates from {len(tasks)} sources")

        return merged

    async def _get_complementary_candidates(
        self,
        user_id: str,
        user_profile: Dict[str, Any],
        excluded_user_ids: List[str],
        override_demand: Optional[str] = None,
    ) -> List[CandidateProfile]:
        """Get candidates via structured complementary skill matching.

        When override_demand is provided, extracts skills from the demand text
        and uses those INSTEAD of profile-level seeking_skills. This ensures
        the user's current intent is prioritized over historical profile data.

        Args:
            user_id: Initiator's user ID
            user_profile: Initiator's profile
            excluded_user_ids: Users to exclude
            override_demand: Explicit demand from user (used to derive seeking_skills)

        Returns:
            List of CandidateProfile objects from complementary matching
        """
        from app.config import settings

        # Determine which skills to use for matching
        if override_demand:
            # Extract skills from the current demand (REPLACE profile skills)
            demand_skills = await self._extract_skills_from_demand(override_demand)
            initiator_seeking = demand_skills.get("seeking_skills") or []
            initiator_seeking_rel = demand_skills.get("seeking_relationship_types") or []

            logger.info(
                f"[ADAPTIVE_MATCHER] Using DEMAND-DERIVED skills for matching:\n"
                f"  - Demand: {override_demand[:80]}...\n"
                f"  - seeking_skills (from demand): {initiator_seeking or '(empty)'}\n"
                f"  - seeking_relationship_types (from demand): {initiator_seeking_rel or '(empty)'}\n"
                f"  - Profile seeking_skills (IGNORED): {user_profile.get('seeking_skills') or '(empty)'}"
            )
        else:
            # Fallback to profile-level skills if no explicit demand
            initiator_seeking = user_profile.get("seeking_skills") or []
            initiator_seeking_rel = user_profile.get("seeking_relationship_types") or []

            logger.info(
                f"[ADAPTIVE_MATCHER] Using PROFILE-LEVEL skills for matching:\n"
                f"  - seeking_skills: {initiator_seeking or '(empty)'}\n"
                f"  - seeking_relationship_types: {initiator_seeking_rel or '(empty)'}"
            )

        # Keep offering_skills from profile (what user can offer doesn't change based on demand)
        initiator_offering = user_profile.get("offering_skills") or []

        # DEBUG: Log the final skill arrays being used
        logger.info(
            f"[ADAPTIVE_MATCHER] Complementary matching for user {user_id[:8]}...\n"
            f"  - seeking_skills: {initiator_seeking or '(empty)'}\n"
            f"  - offering_skills: {initiator_offering or '(empty)'}\n"
            f"  - seeking_relationship_types: {initiator_seeking_rel or '(empty)'}"
        )

        if not initiator_seeking and not initiator_offering:
            logger.warning(
                f"[ADAPTIVE_MATCHER] User {user_id} has no structured skills "
                f"(neither from demand nor profile), skipping complementary matching."
            )
            return []

        match_count = getattr(settings, "complementary_match_count", self.COMPLEMENTARY_MATCH_COUNT)
        logger.debug(f"[ADAPTIVE_MATCHER] Querying for up to {match_count} complementary matches")

        try:
            matches = await self.db.match_users_complementary(
                seeking_skills=initiator_seeking,
                offering_skills=initiator_offering,
                exclude_user_id=user_id,
                exclude_user_ids=excluded_user_ids,
                seeking_relationship_types=initiator_seeking_rel,
                match_count=match_count,
            )
            logger.info(
                f"[ADAPTIVE_MATCHER] DB returned {len(matches)} raw complementary matches "
                f"(excluded {len(excluded_user_ids)} users)"
            )
        except Exception as e:
            logger.error(f"[ADAPTIVE_MATCHER] Complementary matching failed: {e}", exc_info=True)
            return []

        candidates = [
            CandidateProfile(
                user_id=str(match.get("id")),
                name=match.get("name"),
                phone_number=match.get("phone_number"),
                university=match.get("university"),
                major=match.get("major"),
                year=match.get("year"),
                location=match.get("location"),
                career_interests=match.get("career_interests") or [],
                all_demand=match.get("all_demand"),
                all_value=match.get("all_value"),
                context_summary=match.get("context_summary"),
                needs=match.get("needs") or [],
                linkedin_data=match.get("linkedin_data"),
                match_sources={"complementary"},
                similarity_scores={"complementary": match.get("similarity", 0.0)},
            )
            for match in matches
        ]

        # DEBUG: Log each candidate found for traceability
        if candidates:
            for c in candidates[:5]:  # Log first 5 to avoid spam
                logger.debug(
                    f"[ADAPTIVE_MATCHER] Candidate: {c.name} ({c.user_id[:8]}...) "
                    f"university={c.university}, score={c.similarity_scores}"
                )
            if len(candidates) > 5:
                logger.debug(f"[ADAPTIVE_MATCHER] ... and {len(candidates) - 5} more candidates")
        else:
            logger.warning(
                f"[ADAPTIVE_MATCHER] No complementary candidates found. Possible causes:\n"
                f"  - No users have offering_skills matching your seeking_skills: {initiator_seeking}\n"
                f"  - No users have seeking_skills matching your offering_skills: {initiator_offering}\n"
                f"  - All matching users are excluded or not onboarded"
            )

        logger.info(
            f"[ADAPTIVE_MATCHER] Complementary: {len(candidates)} matches "
            f"(seeking={initiator_seeking}, offering={initiator_offering})"
        )

        return candidates

    async def _get_graph_candidates(
        self,
        user_id: str,
        excluded_user_ids: List[str],
    ) -> List[CandidateProfile]:
        """Get candidates via cross-user knowledge graph matching.

        Discovers users connected through shared organizations, domains,
        and skill paths in the knowledge graph.

        Args:
            user_id: Initiator's user ID
            excluded_user_ids: Users to exclude

        Returns:
            List of CandidateProfile objects from graph matching
        """
        from app.config import settings

        match_count = getattr(settings, "graph_match_count", self.GRAPH_MATCH_COUNT)

        try:
            matches = await self.db.match_users_graph(
                user_id=user_id,
                exclude_user_ids=[user_id] + excluded_user_ids,
                match_count=match_count,
            )
        except Exception as e:
            logger.error(f"[ADAPTIVE_MATCHER] Graph matching failed: {e}")
            return []

        candidates = [
            CandidateProfile(
                user_id=str(match.get("id")),
                name=match.get("name"),
                phone_number=match.get("phone_number"),
                university=match.get("university"),
                major=match.get("major"),
                year=match.get("year"),
                location=match.get("location"),
                career_interests=match.get("career_interests") or [],
                all_demand=match.get("all_demand"),
                all_value=match.get("all_value"),
                context_summary=match.get("context_summary"),
                needs=match.get("needs") or [],
                linkedin_data=match.get("linkedin_data"),
                match_sources={"graph"},
                similarity_scores={
                    "graph": min(1.0, (match.get("graph_score", 0) / 5.0)),
                },
            )
            for match in matches
        ]

        logger.info(
            f"[ADAPTIVE_MATCHER] Graph: {len(candidates)} matches"
        )

        return candidates

    def _merge_candidate_pools(
        self,
        results: List[Any],
    ) -> List[CandidateProfile]:
        """Merge and deduplicate candidates from multiple sources.

        When a user appears in multiple sources, their match_sources and
        similarity_scores are combined into a single CandidateProfile.

        Args:
            results: List of candidate pools (or exceptions from asyncio.gather)

        Returns:
            Deduplicated list sorted by source count then best score
        """
        by_user_id: Dict[str, CandidateProfile] = {}

        for pool in results:
            if isinstance(pool, BaseException):
                logger.warning(f"[ADAPTIVE_MATCHER] Candidate source failed: {pool}")
                continue

            for candidate in pool:
                if candidate.user_id in by_user_id:
                    existing = by_user_id[candidate.user_id]
                    # Merge into new candidate (immutable pattern)
                    by_user_id[candidate.user_id] = CandidateProfile(
                        user_id=existing.user_id,
                        name=existing.name,
                        phone_number=existing.phone_number,
                        university=existing.university,
                        major=existing.major,
                        year=existing.year,
                        location=existing.location,
                        career_interests=existing.career_interests,
                        all_demand=existing.all_demand,
                        all_value=existing.all_value,
                        context_summary=existing.context_summary,
                        needs=existing.needs,
                        linkedin_data=existing.linkedin_data,
                        match_sources=existing.match_sources | candidate.match_sources,
                        similarity_scores={
                            **existing.similarity_scores,
                            **candidate.similarity_scores,
                        },
                    )
                else:
                    by_user_id[candidate.user_id] = candidate

        merged = list(by_user_id.values())

        # Sort: multi-source candidates first, then by best similarity score
        merged.sort(
            key=lambda c: (
                len(c.match_sources),
                max(c.similarity_scores.values(), default=0),
            ),
            reverse=True,
        )

        return merged

    async def _fetch_subgraph(
        self,
        initiator_user_id: str,
        candidate_user_ids: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch the graph subgraph for initiator and all candidates.

        Returns edges grouped by source person node's user_id.
        Each edge includes nested target_node with node_type and name.

        Args:
            initiator_user_id: The initiator's user ID
            candidate_user_ids: List of candidate user IDs

        Returns:
            Dict mapping user_id -> list of edge dicts (with nested target_node)
        """
        all_user_ids = [initiator_user_id] + candidate_user_ids

        person_nodes = await self.db.get_person_nodes_for_user_ids(all_user_ids)

        if not person_nodes:
            return {}

        # Build lookup: person_node_id -> user_id
        node_to_user: Dict[str, str] = {}
        for node in person_nodes:
            uid = (node.get("properties") or {}).get("user_id")
            if uid:
                node_to_user[node["id"]] = uid

        person_node_ids = list(node_to_user.keys())

        edges = await self.db.get_subgraph_for_person_nodes(person_node_ids)

        # Group edges by user_id
        edges_by_user: Dict[str, List[Dict[str, Any]]] = {}
        for edge in edges:
            source_user_id = node_to_user.get(edge["source_node_id"])
            if source_user_id:
                edges_by_user.setdefault(source_user_id, []).append(edge)

        logger.info(
            f"[ADAPTIVE_MATCHER] Subgraph fetched: "
            f"{len(person_nodes)} person nodes, {len(edges)} edges, "
            f"{len(edges_by_user)} users with edges"
        )

        return edges_by_user

    def _build_graph_narratives(
        self,
        initiator_user_id: str,
        candidates: List[CandidateProfile],
        edges_by_user: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, GraphNarrative]:
        """Build connection narratives per candidate from shared graph nodes.

        Computes the intersection of initiator's graph neighborhood with each
        candidate's neighborhood, classifies into strength tiers, and formats
        as narrative text for the LLM prompt.

        Args:
            initiator_user_id: The initiator's user ID
            candidates: Candidate profiles
            edges_by_user: Edges grouped by user_id (from _fetch_subgraph)

        Returns:
            Dict mapping candidate user_id -> GraphNarrative
        """
        initiator_edges = edges_by_user.get(initiator_user_id, [])

        # Build initiator's target node set: {(node_type, node_name)} -> edge_type
        initiator_targets: Dict[tuple, str] = {}
        initiator_needs: set = set()

        for edge in initiator_edges:
            target = edge.get("target_node") or {}
            node_type = target.get("node_type", "")
            node_name = target.get("name", "")
            edge_type = edge.get("edge_type", "")
            if node_type and node_name:
                initiator_targets[(node_type, node_name)] = edge_type
                if edge_type == "needs":
                    initiator_needs.add(node_name)

        narratives: Dict[str, GraphNarrative] = {}

        for candidate in candidates:
            candidate_edges = edges_by_user.get(candidate.user_id, [])

            tier1: List[str] = []
            tier2: List[str] = []
            tier3: List[str] = []
            tier4: List[str] = []
            shared_names: List[str] = []

            for edge in candidate_edges:
                target = edge.get("target_node") or {}
                node_type = target.get("node_type", "")
                node_name = target.get("name", "")
                edge_type = edge.get("edge_type", "")

                if not node_type or not node_name:
                    continue

                key = (node_type, node_name)

                # Tier 1: Candidate offers a skill the initiator needs
                if edge_type == "offers" and node_name in initiator_needs:
                    tier1.append(
                        f"{candidate.name} offers '{node_name}' which you need"
                    )
                    shared_names.append(node_name)
                    continue

                # Check if this node is shared with initiator
                if key not in initiator_targets:
                    continue

                shared_names.append(node_name)
                initiator_edge_type = initiator_targets[key]

                # Tier 2: Shared active context (projects, clubs, courses, events)
                active_edge_types = {
                    "works_on", "member_of", "leads",
                    "enrolled_in", "participating_in",
                }
                if node_type in ("project", "course", "event"):
                    if edge_type in active_edge_types:
                        tier2.append(f"Both connected to {node_type} '{node_name}'")
                        continue
                if node_type == "organization" and edge_type in ("member_of", "leads"):
                    tier2.append(f"Both in organization '{node_name}'")
                    continue

                # Tier 3: Shared passive context (university, location, domain)
                if node_type == "location":
                    tier3.append(f"Both located in '{node_name}'")
                    continue
                if node_type == "domain":
                    tier3.append(f"Both interested in domain '{node_name}'")
                    continue
                if node_type == "organization" and initiator_edge_type == "attends":
                    tier3.append(f"Both attend '{node_name}'")
                    continue

                # Tier 4: Parallel paths (same role, same skill sought)
                if node_type in ("role", "skill"):
                    tier4.append(f"Both {edge_type} '{node_name}'")
                    continue

            # Build narrative text
            parts: List[str] = []
            if tier1:
                parts.append(
                    "Direct Demand Match (Tier 1):\n"
                    + "\n".join(f"  - {s}" for s in tier1)
                )
            if tier2:
                parts.append(
                    "Shared Active Context (Tier 2):\n"
                    + "\n".join(f"  - {s}" for s in tier2)
                )
            if tier3:
                parts.append(
                    "Shared Passive Context (Tier 3):\n"
                    + "\n".join(f"  - {s}" for s in tier3)
                )
            if tier4:
                parts.append(
                    "Parallel Paths (Tier 4):\n"
                    + "\n".join(f"  - {s}" for s in tier4)
                )

            narrative_text = "\n".join(parts) if parts else "No graph connections found."

            narratives[candidate.user_id] = GraphNarrative(
                user_id=candidate.user_id,
                tier1_signals=tier1,
                tier2_signals=tier2,
                tier3_signals=tier3,
                tier4_signals=tier4,
                shared_node_names=shared_names,
                narrative_text=narrative_text,
            )

        return narratives

    def _build_graph_prompt_section(
        self,
        candidates: List[CandidateProfile],
        narratives: Dict[str, GraphNarrative],
    ) -> str:
        """Build the candidates section with graph narratives for the LLM prompt.

        Args:
            candidates: Candidate profiles
            narratives: Graph narratives per candidate

        Returns:
            Formatted prompt section string
        """
        section = ""
        for i, candidate in enumerate(candidates, 1):
            profile_text = candidate.to_llm_format(i)
            narrative = narratives.get(candidate.user_id)
            graph_section = ""
            if narrative and narrative.narrative_text != "No graph connections found.":
                graph_section = (
                    f"\n**Graph Connections to Initiator:**\n"
                    f"{narrative.narrative_text}\n"
                )
            section += profile_text + graph_section
        return section

    def _build_initiator_prompt(
        self,
        initiator_profile: Dict[str, Any],
        demand_text: str,
        value_text: Optional[str],
    ) -> str:
        """Build the initiator section of the LLM prompt.

        Args:
            initiator_profile: The initiator's profile
            demand_text: What the initiator is looking for
            value_text: What the initiator can offer

        Returns:
            Formatted initiator prompt section
        """
        name = initiator_profile.get("name", "Unknown")
        university = initiator_profile.get("university", "Not specified")
        major = initiator_profile.get("major", "Not specified")
        year = initiator_profile.get("year", "N/A")
        career_interests = ", ".join(
            initiator_profile.get("career_interests", [])
        ) or "Not specified"
        needs = ", ".join(
            str(n) for n in initiator_profile.get("needs", [])[:3]
        ) or "Not specified"

        context_parts = []
        if grade := initiator_profile.get("year"):
            context_parts.append(f"Grade level: {grade}")
        if location := initiator_profile.get("location"):
            context_parts.append(f"Location: {location}")
        context = "\n".join(context_parts) or "None"

        return f"""## Initiator Profile
Name: {name}
University: {university}
Major: {major}
Year: {year}
Career Interests: {career_interests}
Career Needs: {needs}

### What they're looking for (NETWORKING DEMAND):
{demand_text}

### What they can offer (VALUE):
{value_text or 'Not specified'}

### Additional Context:
{context}"""

    async def _graph_reasoning_select(
        self,
        initiator_profile: Dict[str, Any],
        demand_text: str,
        value_text: Optional[str],
        candidates: List[CandidateProfile],
        narratives: Dict[str, GraphNarrative],
    ) -> Optional[Dict[str, Any]]:
        """Use graph-aware LLM reasoning to select the best match.

        Builds an enhanced prompt with graph narratives appended to each
        candidate's profile, calls LLM with graph reasoning system prompt.

        Args:
            initiator_profile: The initiator's profile
            demand_text: What the initiator is looking for
            value_text: What the initiator can offer
            candidates: Candidate profiles
            narratives: Graph narratives per candidate

        Returns:
            Dict with selection result or None
        """
        if not candidates:
            return None

        initiator_section = self._build_initiator_prompt(
            initiator_profile, demand_text, value_text,
        )
        candidates_section = self._build_graph_prompt_section(
            candidates, narratives,
        )

        user_prompt = f"""{initiator_section}

---

## Candidates to Evaluate (with Graph Connections)
{candidates_section}

---

Based on the initiator's networking demand, the candidate profiles, AND the knowledge graph connections, select the SINGLE BEST match.
Prioritize candidates with multi-path graph connections, especially Tier 1 (direct demand fulfillment) and Tier 2 (shared active context).
Return ONLY valid JSON with your selection."""

        try:
            response = await self.openai.generate_response(
                system_prompt=GRAPH_REASONING_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                temperature=0.3,
                trace_label="graph_reasoning_select",
            )

            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            result = json.loads(cleaned)

            if not result.get("selected_user_id"):
                logger.error("[ADAPTIVE_MATCHER] Graph reasoning response missing selected_user_id")
                return None

            logger.info(
                f"[ADAPTIVE_MATCHER] Graph reasoning selected: {result.get('selected_user_id')} "
                f"(confidence: {result.get('confidence', 0.0):.2f}, "
                f"paths: {result.get('graph_paths_cited', [])})"
            )

            return result

        except json.JSONDecodeError as e:
            logger.error(f"[ADAPTIVE_MATCHER] Failed to parse graph reasoning response: {e}")
            return None
        except Exception as e:
            logger.error(f"[ADAPTIVE_MATCHER] Graph reasoning selection failed: {e}", exc_info=True)
            return None

    async def _graph_reasoning_select_multiple(
        self,
        initiator_profile: Dict[str, Any],
        demand_text: str,
        value_text: Optional[str],
        candidates: List[CandidateProfile],
        narratives: Dict[str, GraphNarrative],
        select_count: int = 3,
    ) -> List[Dict[str, Any]]:
        """Use graph-aware LLM reasoning to select N matches in one call.

        For group chat formation — selects multiple complementary matches
        considering group synergy and diversity.

        Args:
            initiator_profile: The initiator's profile
            demand_text: What the initiator is looking for
            value_text: What the initiator can offer
            candidates: Candidate profiles
            narratives: Graph narratives per candidate
            select_count: Number of matches to select

        Returns:
            List of selection dicts, or empty list on failure
        """
        if not candidates:
            return []

        initiator_section = self._build_initiator_prompt(
            initiator_profile, demand_text, value_text,
        )
        candidates_section = self._build_graph_prompt_section(
            candidates, narratives,
        )

        user_prompt = f"""{initiator_section}

---

## Candidates to Evaluate (with Graph Connections)
{candidates_section}

---

Select the BEST {select_count} matches for a GROUP connection. Consider:
1. Each person should bring DIFFERENT value to the group
2. Prefer candidates who share context with each other (not just the initiator)
3. The group as a whole should cover the initiator's demand comprehensively
Return ONLY valid JSON."""

        try:
            response = await self.openai.generate_response(
                system_prompt=GRAPH_REASONING_MULTI_SELECT_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                temperature=0.3,
                trace_label="graph_reasoning_select_multiple",
            )

            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            result = json.loads(cleaned)
            selections = result.get("selections", [])

            if not selections:
                logger.error("[ADAPTIVE_MATCHER] Graph reasoning multi-select returned no selections")
                return []

            valid = [s for s in selections if s.get("selected_user_id")]

            logger.info(
                f"[ADAPTIVE_MATCHER] Graph reasoning multi-selected {len(valid)} matches "
                f"(group_synergy: {result.get('group_synergy', 'N/A')[:80]})"
            )

            return valid[:select_count]

        except json.JSONDecodeError as e:
            logger.error(f"[ADAPTIVE_MATCHER] Failed to parse graph reasoning multi-select: {e}")
            return []
        except Exception as e:
            logger.error(f"[ADAPTIVE_MATCHER] Graph reasoning multi-select failed: {e}", exc_info=True)
            return []

    async def _select_match(
        self,
        initiator_profile: Dict[str, Any],
        initiator_user_id: str,
        demand_text: str,
        value_text: Optional[str],
        candidates: List[CandidateProfile],
    ) -> Optional[Dict[str, Any]]:
        """Select a match using graph reasoning only.

        Graph-first contract:
        - Graph reasoning must be enabled.
        - Initiator must have graph data in the candidate subgraph.
        - No fallback to legacy profile-only selection.

        Args:
            initiator_profile: The initiator's profile
            initiator_user_id: The initiator's user ID
            demand_text: What the initiator is looking for
            value_text: What the initiator can offer
            candidates: Candidate profiles

        Returns:
            Dict with selection result or None
        """
        from app.config import settings

        if not getattr(settings, "graph_reasoning_enabled", False):
            logger.error(
                "[ADAPTIVE_MATCHER] Graph-first contract violation: "
                "graph_reasoning_enabled is False"
            )
            return None

        try:
            edges_by_user = await self._fetch_subgraph(
                initiator_user_id=initiator_user_id,
                candidate_user_ids=[c.user_id for c in candidates],
            )
        except Exception as e:
            logger.error(
                f"[ADAPTIVE_MATCHER] Graph reasoning subgraph fetch failed: {e}",
                exc_info=True,
            )
            return None

        if not edges_by_user.get(initiator_user_id):
            # Fallback: Use LLM-based selection without graph context
            logger.info(
                f"[ADAPTIVE_MATCHER] No graph edges for initiator {initiator_user_id[:8]}..., "
                f"falling back to LLM-based selection"
            )
            return await self._llm_select_best_match(
                initiator_profile=initiator_profile,
                demand_text=demand_text,
                value_text=value_text,
                candidates=candidates,
            )

        narratives = self._build_graph_narratives(
            initiator_user_id=initiator_user_id,
            candidates=candidates,
            edges_by_user=edges_by_user,
        )

        return await self._graph_reasoning_select(
            initiator_profile=initiator_profile,
            demand_text=demand_text,
            value_text=value_text,
            candidates=candidates,
            narratives=narratives,
        )

    async def find_best_matches(
        self,
        user_id: str,
        user_profile: Dict[str, Any],
        excluded_user_ids: Optional[List[str]] = None,
        override_demand: Optional[str] = None,
        override_value: Optional[str] = None,
        select_count: int = 3,
    ) -> List[AdaptiveMatchResult]:
        """Find multiple best matches in a single LLM call using graph reasoning.

        Batch alternative to calling find_best_match in a loop. Uses graph-aware
        multi-select to pick N complementary candidates at once.

        Args:
            user_id: The initiator's user ID
            user_profile: The initiator's profile data
            excluded_user_ids: User IDs to exclude
            override_demand: Override the user's demand
            override_value: Override the user's value
            select_count: Number of matches to return (max 3)

        Returns:
            List of AdaptiveMatchResult (may be fewer than select_count)
        """
        from app.config import settings

        select_count = min(select_count, 3)
        excluded = excluded_user_ids or []

        demand_text = (
            override_demand
            or user_profile.get("latest_demand")
            or user_profile.get("all_demand")
        )
        value_text = override_value or user_profile.get("all_value")

        if not demand_text:
            return [AdaptiveMatchResult(
                success=False,
                error_message="No demand specified. What kind of help are you looking for?",
            )]

        if not getattr(settings, "graph_reasoning_enabled", False):
            return [AdaptiveMatchResult(
                success=False,
                error_message=(
                    "Graph-first matching is enabled, but graph_reasoning_enabled is off."
                ),
            )]

        # Generate candidate pool (same as single match)
        candidates = await self._generate_candidate_pool(
            user_id=user_id,
            user_profile=user_profile,
            excluded_user_ids=excluded,
            override_demand=override_demand,
        )

        if not candidates:
            return [AdaptiveMatchResult(
                success=False,
                error_message="No suitable matches found at this time.",
            )]

        # Enrich candidates
        candidates = await self._enrich_candidates_with_zep(
            candidates=candidates,
            initiator_demand=demand_text,
        )
        candidates = await self._enrich_candidates_with_profiles(
            candidates=candidates,
        )

        top_candidates = candidates[:self.CANDIDATE_POOL_SIZE]

        try:
            edges_by_user = await self._fetch_subgraph(
                initiator_user_id=user_id,
                candidate_user_ids=[c.user_id for c in top_candidates],
            )
        except Exception as e:
            logger.error(
                f"[ADAPTIVE_MATCHER] Graph reasoning batch subgraph fetch failed: {e}",
                exc_info=True,
            )
            return [AdaptiveMatchResult(
                success=False,
                error_message="Failed to fetch graph context for batch matching.",
            )]

        if not edges_by_user.get(user_id):
            # Fallback: Use LLM-based selection without graph context
            # This allows matching even when user hasn't synced email or graph isn't populated
            logger.info(
                f"[ADAPTIVE_MATCHER] No graph edges for initiator {user_id[:8]}..., "
                f"falling back to LLM-based selection for {len(top_candidates)} candidates"
            )

            # Use LLM to select multiple candidates without graph reasoning
            results: List[AdaptiveMatchResult] = []
            remaining_candidates = list(top_candidates)

            for _ in range(min(select_count, len(top_candidates))):
                if not remaining_candidates:
                    break

                selection = await self._llm_select_best_match(
                    initiator_profile=user_profile,
                    demand_text=demand_text,
                    value_text=value_text,
                    candidates=remaining_candidates,
                )

                if selection and selection.get("selected_user_id"):
                    selected_id = selection["selected_user_id"]
                    selected = next(
                        (c for c in remaining_candidates if c.user_id == selected_id),
                        None,
                    )
                    if selected:
                        results.append(AdaptiveMatchResult(
                            success=True,
                            target_user_id=selected.user_id,
                            target_name=selected.name,
                            target_phone=selected.phone_number,
                            target_profile=selected.to_dict(),
                            matching_reasons=selection.get("matching_reasons", []),
                            llm_introduction=selection.get("personalized_intro", ""),
                            llm_concern=selection.get("potential_concerns", ""),
                            match_score=selected.complementary_score,
                            match_confidence="medium",  # Lower confidence without graph
                        ))
                        # Remove selected from candidates to avoid duplicates
                        remaining_candidates = [
                            c for c in remaining_candidates if c.user_id != selected_id
                        ]

            if results:
                logger.info(
                    f"[ADAPTIVE_MATCHER] LLM fallback selected {len(results)} matches"
                )
                return results

            return [AdaptiveMatchResult(
                success=False,
                error_message="Could not find suitable matches. Please try again.",
            )]

        narratives = self._build_graph_narratives(
            initiator_user_id=user_id,
            candidates=top_candidates,
            edges_by_user=edges_by_user,
        )

        selections = await self._graph_reasoning_select_multiple(
            initiator_profile=user_profile,
            demand_text=demand_text,
            value_text=value_text,
            candidates=top_candidates,
            narratives=narratives,
            select_count=select_count,
        )

        if not selections:
            return [AdaptiveMatchResult(
                success=False,
                error_message=(
                    "Could not determine graph-grounded group matches. "
                    "Please try again."
                ),
            )]

        results: List[AdaptiveMatchResult] = []
        seen_selected_ids: Set[str] = set()
        for sel in selections:
            selected_id = sel.get("selected_user_id")
            if not selected_id or selected_id in seen_selected_ids:
                continue
            selected = next(
                (c for c in top_candidates if c.user_id == selected_id),
                None,
            )
            if not selected:
                continue
            seen_selected_ids.add(selected_id)

            rationale = sel.get("rationale", {})
            matching_reasons = self._build_matching_reasons(selected, rationale)

            results.append(AdaptiveMatchResult(
                success=True,
                target_user_id=selected.user_id,
                target_name=selected.name,
                target_phone=selected.phone_number,
                match_confidence=sel.get("confidence", 0.0),
                match_score=sel.get("confidence", 0.0),
                demand_satisfaction=rationale.get("demand_satisfaction"),
                mutual_benefit=rationale.get("mutual_benefit"),
                match_summary=sel.get("match_summary"),
                concern=sel.get("concern"),
                matching_reasons=matching_reasons,
                llm_introduction=sel.get("match_summary"),
                llm_concern=sel.get("concern"),
            ))

        if results:
            logger.info(
                f"[ADAPTIVE_MATCHER] Batch graph reasoning: "
                f"{len(results)} matches selected"
            )
            return results

        return [AdaptiveMatchResult(
            success=False,
            error_message="Graph reasoning returned invalid candidate selections.",
        )]

    async def _enrich_candidates_with_zep(
        self,
        candidates: List[CandidateProfile],
        initiator_demand: str,
        max_facts: int = 3,
    ) -> List[CandidateProfile]:
        """Enrich candidates with Zep knowledge graph facts.

        For each candidate, searches their Zep knowledge graph for facts
        relevant to the initiator's demand. This provides additional context
        for LLM-based match selection.

        Args:
            candidates: List of candidates to enrich
            initiator_demand: The initiator's networking demand
            max_facts: Maximum facts to fetch per candidate

        Returns:
            List of candidates with zep_facts populated
        """
        from app.config import settings

        if not getattr(settings, 'zep_graph_enabled', False):
            return candidates

        if not getattr(settings, 'zep_graph_enrich_candidates', True):
            return candidates

        from app.agents.tools.user_context import search_user_context

        async def enrich_one(candidate: CandidateProfile) -> CandidateProfile:
            try:
                facts = await search_user_context(
                    user_id=candidate.user_id,
                    query=initiator_demand,
                    limit=max_facts,
                )
                if facts:
                    # Create new candidate with zep_facts (immutable pattern)
                    # Preserve all fields including holistic profile data
                    return CandidateProfile(
                        user_id=candidate.user_id,
                        name=candidate.name,
                        phone_number=candidate.phone_number,
                        university=candidate.university,
                        major=candidate.major,
                        year=candidate.year,
                        location=candidate.location,
                        career_interests=candidate.career_interests,
                        all_demand=candidate.all_demand,
                        all_value=candidate.all_value,
                        context_summary=candidate.context_summary,
                        needs=candidate.needs,
                        linkedin_data=candidate.linkedin_data,
                        match_sources=candidate.match_sources,
                        similarity_scores=candidate.similarity_scores,
                        zep_facts=facts,
                        # Preserve holistic profile fields
                        holistic_summary=candidate.holistic_summary,
                        latent_needs=candidate.latent_needs,
                        ideal_relationship_types=candidate.ideal_relationship_types,
                        career_stage=candidate.career_stage,
                        relationship_strengths=candidate.relationship_strengths,
                    )
                return candidate
            except Exception as e:
                logger.debug(
                    f"[ADAPTIVE_MATCHER] Zep enrichment failed for {candidate.user_id[:8]}: {e}"
                )
                return candidate

        # Enrich top candidates in parallel
        pool_size = min(len(candidates), self.CANDIDATE_POOL_SIZE)
        tasks = [enrich_one(c) for c in candidates[:pool_size]]
        enriched = await asyncio.gather(*tasks)

        # Replace enriched candidates, keep rest unchanged
        result = list(enriched) + candidates[pool_size:]

        enriched_count = sum(1 for c in enriched if c.zep_facts)
        if enriched_count > 0:
            logger.info(
                f"[ADAPTIVE_MATCHER] Enriched {enriched_count}/{pool_size} candidates with Zep facts"
            )

        return result

    async def _enrich_candidates_with_profiles(
        self,
        candidates: List[CandidateProfile],
    ) -> List[CandidateProfile]:
        """Enrich candidates with holistic profile data.

        For candidates that don't already have holistic profile data
        (i.e., came from non-holistic queries), fetch their profiles
        from the database.

        Args:
            candidates: List of candidates to enrich

        Returns:
            List of candidates with holistic profile data populated
        """
        from app.config import settings

        if not getattr(settings, "profile_synthesis_use_in_matching", True):
            return candidates

        async def enrich_one(candidate: CandidateProfile) -> CandidateProfile:
            if candidate.holistic_summary:
                return candidate

            try:
                profile = await self.db.get_user_profile(candidate.user_id)
                if profile:
                    return CandidateProfile(
                        user_id=candidate.user_id,
                        name=candidate.name,
                        phone_number=candidate.phone_number,
                        university=candidate.university,
                        major=candidate.major,
                        year=candidate.year,
                        location=candidate.location,
                        career_interests=candidate.career_interests,
                        all_demand=candidate.all_demand,
                        all_value=candidate.all_value,
                        context_summary=candidate.context_summary,
                        needs=candidate.needs,
                        linkedin_data=candidate.linkedin_data,
                        match_sources=candidate.match_sources,
                        similarity_scores=candidate.similarity_scores,
                        zep_facts=candidate.zep_facts,
                        holistic_summary=profile.get("holistic_summary"),
                        latent_needs=profile.get("latent_needs") or [],
                        ideal_relationship_types=profile.get("ideal_relationship_types") or [],
                        career_stage=profile.get("career_stage"),
                        relationship_strengths=profile.get("relationship_strengths"),
                    )
                return candidate
            except Exception as e:
                logger.debug(
                    f"[ADAPTIVE_MATCHER] Profile enrichment failed for {candidate.user_id[:8]}: {e}"
                )
                return candidate

        pool_size = min(len(candidates), self.CANDIDATE_POOL_SIZE)
        tasks = [enrich_one(c) for c in candidates[:pool_size]]
        enriched = await asyncio.gather(*tasks)

        result = list(enriched) + candidates[pool_size:]

        enriched_count = sum(1 for c in enriched if c.holistic_summary)
        if enriched_count > 0:
            logger.info(
                f"[ADAPTIVE_MATCHER] Enriched {enriched_count}/{pool_size} candidates with holistic profiles"
            )

        return result

    async def _llm_select_best_match(
        self,
        initiator_profile: Dict[str, Any],
        demand_text: str,
        value_text: Optional[str],
        candidates: List[CandidateProfile],
    ) -> Optional[Dict[str, Any]]:
        """Use LLM to select the best match from candidates.

        Args:
            initiator_profile: The initiator's profile
            demand_text: What the initiator is looking for
            value_text: What the initiator can offer
            candidates: List of candidate profiles to evaluate

        Returns:
            Dict with selection result or None if failed
        """
        if not candidates:
            return None

        # Build initiator context
        initiator_name = initiator_profile.get("name", "Unknown")
        initiator_university = initiator_profile.get("university", "Not specified")
        initiator_major = initiator_profile.get("major", "Not specified")
        initiator_year = initiator_profile.get("year", "N/A")
        initiator_career_interests = ", ".join(
            initiator_profile.get("career_interests", [])
        ) or "Not specified"
        initiator_needs = ", ".join(
            str(n) for n in initiator_profile.get("needs", [])[:3]
        ) or "Not specified"

        # Build additional context
        initiator_context_parts = []
        if grade := initiator_profile.get("year"):
            initiator_context_parts.append(f"Grade level: {grade}")
        if location := initiator_profile.get("location"):
            initiator_context_parts.append(f"Location: {location}")
        initiator_context = "\n".join(initiator_context_parts) or "None"

        # Build candidates section
        candidates_section = ""
        for i, candidate in enumerate(candidates, 1):
            candidates_section += candidate.to_llm_format(i)

        user_prompt = f"""## Initiator Profile
Name: {initiator_name}
University: {initiator_university}
Major: {initiator_major}
Year: {initiator_year}
Career Interests: {initiator_career_interests}
Career Needs: {initiator_needs}

### What they're looking for (NETWORKING DEMAND):
{demand_text}

### What they can offer (VALUE):
{value_text or 'Not specified'}

### Additional Context:
{initiator_context}

---

## Candidates to Evaluate
{candidates_section}

---

Based on the initiator's networking demand and the available candidates, select the SINGLE BEST match that:
1. Best satisfies what the initiator is looking for
2. Has clear motivation to engage (mutual benefit)

Return ONLY valid JSON with your selection."""

        try:
            response = await self.openai.generate_response(
                system_prompt=MATCH_SELECTION_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                temperature=0.3,
                trace_label="adaptive_match_selection",
            )

            # Parse JSON response
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            result = json.loads(cleaned)

            # Validate result has required fields
            if not result.get("selected_user_id"):
                logger.error("[ADAPTIVE_MATCHER] LLM response missing selected_user_id")
                return None

            logger.info(
                f"[ADAPTIVE_MATCHER] LLM selected: {result.get('selected_user_id')} "
                f"(confidence: {result.get('confidence', 0.0):.2f})"
            )

            return result

        except json.JSONDecodeError as e:
            logger.error(f"[ADAPTIVE_MATCHER] Failed to parse LLM response: {e}")
            logger.error(f"[ADAPTIVE_MATCHER] Raw response: {response[:500]}")
            return None
        except Exception as e:
            logger.error(f"[ADAPTIVE_MATCHER] LLM selection failed: {e}", exc_info=True)
            return None

    def _build_matching_reasons(
        self,
        candidate: CandidateProfile,
        rationale: Dict[str, str],
    ) -> List[str]:
        """Build human-readable matching reasons.

        Args:
            candidate: The selected candidate
            rationale: LLM's rationale for the selection

        Returns:
            List of matching reason strings
        """
        reasons = []

        # Add demand satisfaction reason
        if demand_sat := rationale.get("demand_satisfaction"):
            reasons.append(demand_sat)

        # Add mutual benefit reason
        if mutual := rationale.get("mutual_benefit"):
            reasons.append(mutual)

        # Add context if relevant
        if context := rationale.get("context_fit"):
            reasons.append(context)

        # Fallback to basic reasons if rationale is empty
        if not reasons:
            if candidate.all_value:
                reasons.append(f"Can help with: {candidate.all_value[:100]}...")
            if candidate.career_interests:
                careers = ", ".join(candidate.career_interests[:3])
                reasons.append(f"Background in: {careers}")
            if candidate.university:
                reasons.append(f"From {candidate.university}")

        return reasons[:3]
