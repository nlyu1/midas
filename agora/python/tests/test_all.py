import pytest
import trial


def test_sum_as_string():
    assert trial.sum_as_string(1, 1) == "2"
