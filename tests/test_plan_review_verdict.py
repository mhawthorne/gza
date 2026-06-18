"""Tests for plan-review verdict and slice-manifest parsing."""

import copy
import json

import pytest

from gza.plan_review_verdict import (
    PlanReviewValidationError,
    SLICE_COMPLEXITIES,
    parse_plan_review_report,
    parse_plan_review_verdict,
    validate_plan_review_report,
)


def _base_manifest() -> dict:
    return {
        "schema_version": 1,
        "source_task_id": "gza-123",
        "source_task_type": "plan",
        "verdict": "APPROVED",
        "slice_quality": {
            "fits_single_task_budget": True,
            "timeout_budget_minutes": 45,
            "max_expected_files_changed_per_slice": 12,
            "rationale": "Each slice stays within one task budget.",
        },
        "slices": [
            {
                "slice_id": "S1",
                "title": "Foundation",
                "prompt": "Implement the foundation slice.",
                "scope": ["Add task types", "Add config keys"],
                "out_of_scope": ["Lifecycle rule changes"],
                "acceptance_criteria": ["Task types validate", "Config loads"],
                "depends_on_slices": [],
                "based_on_slice": None,
                "review_scope": "Foundation only.",
                "estimated_complexity": "medium",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
            {
                "slice_id": "S2",
                "title": "Parser",
                "prompt": "Implement the parser slice.",
                "scope": ["Parse verdict", "Validate manifest"],
                "out_of_scope": ["Executor changes"],
                "acceptance_criteria": ["Approved manifest parses"],
                "depends_on_slices": ["S1"],
                "based_on_slice": "S1",
                "review_scope": "Parser only.",
                "estimated_complexity": "large",
                "expected_timeout_minutes": 45,
                "requires_code_review": True,
                "tags": ["lifecycle", "parser"],
            },
        ],
        "manual_override": {
            "commands": [
                "uv run gza plan-review <review-id> --edit-slices",
            ],
        },
    }


def _report_for_manifest(manifest: dict) -> str:
    import json

    return (
        "## Verdict\n"
        "Verdict: APPROVED\n\n"
        "## Slice Manifest\n"
        "```json\n"
        f"{json.dumps(manifest, indent=2)}\n"
        "```\n"
    )


def _report_with_authoritative_verdict_and_manifest_example(
    *,
    quoted_verdict: str,
    final_verdict: str,
) -> str:
    manifest = _base_manifest()
    manifest["verdict"] = final_verdict
    return (
        "## Guidance\n"
        f"Use `Verdict: {quoted_verdict}` only as an example.\n\n"
        "## Verdict\n\n"
        f"{final_verdict}\n\n"
        "## Slice Manifest\n"
        "```json\n"
        f"{json.dumps(manifest, indent=2)}\n"
        "```\n"
    )


class TestParsePlanReviewVerdict:
    def test_parses_inline_and_heading_verdicts(self) -> None:
        assert parse_plan_review_verdict("Verdict: APPROVED") == "APPROVED"
        assert parse_plan_review_verdict("## Verdict\n\nCHANGES_REQUESTED\n") == "CHANGES_REQUESTED"

    @pytest.mark.parametrize(
        ("quoted_verdict", "final_verdict"),
        [
            ("CHANGES_REQUESTED", "APPROVED"),
            ("APPROVED", "CHANGES_REQUESTED"),
        ],
    )
    def test_prefers_authoritative_final_verdict_section_over_earlier_example_token(
        self,
        quoted_verdict: str,
        final_verdict: str,
    ) -> None:
        content = (
            "## Notes\n"
            f'Example output may include "Verdict: {quoted_verdict}" in prose.\n\n'
            "## Verdict\n\n"
            f"{final_verdict}\n"
        )

        assert parse_plan_review_verdict(content) == final_verdict

    def test_parse_report_extracts_single_json_manifest(self) -> None:
        report = parse_plan_review_report(_report_for_manifest(_base_manifest()))
        assert report.verdict == "APPROVED"
        assert report.raw_manifest is not None
        assert report.raw_manifest["source_task_id"] == "gza-123"

    @pytest.mark.parametrize(
        ("quoted_verdict", "final_verdict"),
        [
            ("CHANGES_REQUESTED", "APPROVED"),
            ("APPROVED", "CHANGES_REQUESTED"),
        ],
    )
    def test_parse_report_uses_authoritative_final_verdict_section(
        self,
        quoted_verdict: str,
        final_verdict: str,
    ) -> None:
        report = parse_plan_review_report(
            _report_with_authoritative_verdict_and_manifest_example(
                quoted_verdict=quoted_verdict,
                final_verdict=final_verdict,
            )
        )

        assert report.verdict == final_verdict


class TestValidatePlanReviewReport:
    def test_accepts_valid_approved_manifest(self) -> None:
        manifest = validate_plan_review_report(
            _report_for_manifest(_base_manifest()),
            source_task_id="gza-123",
            source_task_type="plan",
            max_slice_timeout_minutes=45,
        )

        assert manifest is not None
        assert manifest.verdict == "APPROVED"
        assert [slice_manifest.slice_id for slice_manifest in manifest.slices] == ["S1", "S2"]

    def test_coerces_string_scope_fields_in_approved_manifest(self) -> None:
        manifest = copy.deepcopy(_base_manifest())
        manifest["slices"][0]["scope"] = "Add task types"
        manifest["slices"][0]["out_of_scope"] = "Lifecycle rule changes"

        validated = validate_plan_review_report(
            _report_for_manifest(manifest),
            source_task_id="gza-123",
            source_task_type="plan",
            max_slice_timeout_minutes=45,
        )

        assert validated is not None
        assert validated.slices[0].scope == ("Add task types",)
        assert validated.slices[0].out_of_scope == ("Lifecycle rule changes",)

    @pytest.mark.parametrize("estimated_complexity", sorted(SLICE_COMPLEXITIES))
    def test_accepts_each_allowed_estimated_complexity(self, estimated_complexity: str) -> None:
        manifest = copy.deepcopy(_base_manifest())
        manifest["slices"][0]["estimated_complexity"] = estimated_complexity

        validated = validate_plan_review_report(
            _report_for_manifest(manifest),
            source_task_id="gza-123",
            source_task_type="plan",
            max_slice_timeout_minutes=45,
        )

        assert validated is not None
        assert validated.slices[0].estimated_complexity == estimated_complexity

    def test_rejects_out_of_enum_estimated_complexity(self) -> None:
        manifest = copy.deepcopy(_base_manifest())
        manifest["slices"][0]["estimated_complexity"] = "high"

        with pytest.raises(
            PlanReviewValidationError,
            match=r"slice S1\.estimated_complexity must be one of",
        ):
            validate_plan_review_report(
                _report_for_manifest(manifest),
                source_task_id="gza-123",
                source_task_type="plan",
                max_slice_timeout_minutes=45,
            )

    @pytest.mark.parametrize(
        ("mutate", "message"),
        [
            (lambda manifest: manifest.update({"slices": []}), "at least one slice"),
            (
                lambda manifest: manifest["slices"].__setitem__(
                    1,
                    {
                        **manifest["slices"][1],
                        "depends_on_slices": ["S2"],
                    },
                ),
                "earlier slices",
            ),
            (
                lambda manifest: manifest["slices"].__setitem__(
                    1,
                    {
                        **manifest["slices"][1],
                        "depends_on_slices": ["S9"],
                    },
                ),
                "unknown dependency",
            ),
            (
                lambda manifest: manifest["slices"][0].pop("acceptance_criteria"),
                "acceptance_criteria",
            ),
            (
                lambda manifest: manifest["slices"].__setitem__(
                    1,
                    {
                        **manifest["slices"][1],
                        "expected_timeout_minutes": 46,
                    },
                ),
                "exceeds plan slice timeout budget",
            ),
            (
                lambda manifest: manifest["slices"].__setitem__(
                    1,
                    {
                        **manifest["slices"][1],
                        "depends_on_slices": ["S1", "S0"],
                    },
                ),
                "single-dependency limit",
            ),
            (
                lambda manifest: manifest["slices"].__setitem__(
                    0,
                    {
                        **manifest["slices"][0],
                        "requires_code_review": False,
                    },
                ),
                "requires_code_review=true",
            ),
            (
                lambda manifest: manifest["slice_quality"].update({"fits_single_task_budget": False}),
                "fits_single_task_budget=true",
            ),
        ],
    )
    def test_rejects_invalid_approved_manifests(self, mutate, message: str) -> None:
        manifest = copy.deepcopy(_base_manifest())
        mutate(manifest)

        with pytest.raises(PlanReviewValidationError, match=message):
            validate_plan_review_report(
                _report_for_manifest(manifest),
                source_task_id="gza-123",
                source_task_type="plan",
                max_slice_timeout_minutes=45,
            )

    def test_rejects_source_task_id_mismatch(self) -> None:
        manifest = copy.deepcopy(_base_manifest())
        manifest["source_task_id"] = "gza-999"

        with pytest.raises(PlanReviewValidationError, match="does not match"):
            validate_plan_review_report(
                _report_for_manifest(manifest),
                source_task_id="gza-123",
                source_task_type="plan",
                max_slice_timeout_minutes=45,
            )

    @pytest.mark.parametrize(
        "depends_on_slices",
        [
            [],
        ],
    )
    def test_rejects_based_on_slice_without_matching_single_dependency(
        self,
        depends_on_slices: list[str],
    ) -> None:
        manifest = copy.deepcopy(_base_manifest())
        manifest["slices"][1]["depends_on_slices"] = depends_on_slices

        with pytest.raises(
            PlanReviewValidationError,
            match="must list based_on_slice S1 as its only depends_on_slices entry",
        ):
            validate_plan_review_report(
                _report_for_manifest(manifest),
                source_task_id="gza-123",
                source_task_type="plan",
                max_slice_timeout_minutes=45,
            )

    def test_rejects_based_on_slice_with_contradictory_dependency(self) -> None:
        manifest = copy.deepcopy(_base_manifest())
        manifest["slices"].insert(
            1,
            {
                "slice_id": "S1b",
                "title": "Intermediate",
                "prompt": "Implement the intermediate slice.",
                "scope": ["Add helper"],
                "out_of_scope": [],
                "acceptance_criteria": ["Helper works"],
                "depends_on_slices": ["S1"],
                "based_on_slice": None,
                "review_scope": "Intermediate only.",
                "estimated_complexity": "small",
                "expected_timeout_minutes": 30,
                "requires_code_review": True,
                "tags": ["lifecycle"],
            },
        )
        manifest["slices"][2]["depends_on_slices"] = ["S1b"]

        with pytest.raises(
            PlanReviewValidationError,
            match="must list based_on_slice S1 as its only depends_on_slices entry",
        ):
            validate_plan_review_report(
                _report_for_manifest(manifest),
                source_task_id="gza-123",
                source_task_type="plan",
                max_slice_timeout_minutes=45,
            )
