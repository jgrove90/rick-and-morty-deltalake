import sys
import os
import numpy as np

# Append the path to your module directory to sys.path
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.insert(0, module_path)

from app_utils import getID, dateTimeFormat


def test_getID_dict():
    data = {"url": "https://example.com/123"}
    result = getID(data)
    assert result == 123


def test_getID_array():
    data = np.array(["https://example.com/456", "https://example.com/789"])
    result = getID(data)
    assert result == [456, 789]


def test_dateTimeFormat():
    date_str = "2023-07-18T12:34:56.789Z"
    formatted_date = dateTimeFormat(date_str)
    assert formatted_date == "2023-07-18 12:34:56"
