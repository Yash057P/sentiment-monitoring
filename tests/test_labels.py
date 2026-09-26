"""Tests for label conversion and prediction-to-label conversion."""

import pytest


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("0", 0),
        ("4", 1),
        (0, 0),
        (4, 1),
        (None, None),
        ("2", None),
        ("negative", None),
    ],
)
def test_target_to_label(raw, expected):
    from dataset_utils import target_to_label

    assert target_to_label(raw) == expected


def test_target_to_label_strips_whitespace():
    from dataset_utils import target_to_label

    assert target_to_label(" 4 ") == 1
    assert target_to_label("0 ") == 0


def test_target_to_label_mapping_matches_binary_classes():
    """0 -> Negative, 4 -> Positive are the only legitimate classes."""
    from dataset_utils import TARGET_TO_LABEL

    assert TARGET_TO_LABEL == {"0": 0, "4": 1}


@pytest.mark.parametrize(
    "raw,expected",
    [
        (0.0, "Negative"),
        (1.0, "Positive"),
        (0.23, "Negative"),
        (0.77, "Positive"),
    ],
)
def test_prediction_to_label(raw, expected):
    from dataset_utils import prediction_to_label

    assert prediction_to_label(raw) == expected