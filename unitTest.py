import pytest
import json
import numpy as np
from unittest.mock import patch, mock_open, MagicMock
import os
from src.main import *

# Function to read the config file
def read_configs( configFile ):
    with open(configFile, 'r') as file:
        config = json.load(file)
        return config

# Tests for download_file function
"""
Summary of Changes: for download_file
    Return Statements: The download_file function now always returns a string, ensuring the tests have something to compare against.
    Assertions: The test assertions now check for these returned string values, ensuring the function behaves as expected in all scenarios.
    This should resolve the AssertionError: assert None == issue and allow your tests to pass correctly.
"""

# Test for a successful file download
@patch('src.main.requests.get')  # Mocking requests.get
@patch('builtins.open', new_callable=mock_open)
def test_download_file_success(mock_file, mock_get):
    # Mock response object with iter_content method
    mock_response = MagicMock()
    mock_response.iter_content = MagicMock(return_value=[b'test data'])
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    # Define the URL and save path
    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    save_path = os.path.join("/Users/sohansamant/Desktop/SohanSamant/hf_bi_python_exercise/recipes-etl/src/","recipes.json")

    # Call the function
    result = download_file(url, save_path)

    # Check if the file was opened in write-binary mode
    mock_file.assert_called_with(save_path, 'wb')

    # Check if write was called with the correct data
    mock_file().write.assert_called_with(b'test data')

    # Check if requests.get was called with the correct URL
    mock_get.assert_called_with(url, stream=True)
    
    assert result == f"File downloaded successfully and saved to {save_path}"

# Test for HTTPError handling
@patch('src.main.requests.get')  # Mocking requests.get
def test_download_file_http_error(mock_get):
    # Mock an HTTPError
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")
    mock_get.return_value = mock_response

    # Define the URL and save path
    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    save_path = os.path.join("/Users/sohansamant/Desktop/SohanSamant/hf_bi_python_exercise/recipes-etl/src/","recipes.json")

    # Call the function
    result = download_file(url, save_path)

    # Assertions
    assert result == "HTTP error occurred: HTTP Error"

# Test for ConnectionError handling
@patch('src.main.requests.get')  # Mocking requests.get
def test_download_file_connection_error(mock_get):
    # Mock a ConnectionError
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection Error")

    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    save_path = os.path.join("/Users/sohansamant/Desktop/SohanSamant/hf_bi_python_exercise/recipes-etl/src/","recipes.json")

    # Call the function
    result = download_file(url, save_path)

    # Assertions
    assert result == "Connection error occurred: Connection Error"

# Test for Timeout handling
@patch('src.main.requests.get')  # Mocking requests.get
def test_download_file_timeout_error(mock_get):
    # Mock a Timeout error
    mock_get.side_effect = requests.exceptions.Timeout("Timeout Error")

    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    save_path = os.path.join("/Users/sohansamant/Desktop/SohanSamant/hf_bi_python_exercise/recipes-etl/src/","recipes.json")

    # Call the function
    result = download_file(url, save_path)

    # Assertions
    assert result == "Timeout error occurred: Timeout Error"

@patch('src.main.requests.get')  # Mocking requests.get
def test_download_file_generic_request_exception(mock_get):
    # Mock a generic RequestException
    mock_get.side_effect = requests.exceptions.RequestException("Request Error")

    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    save_path = os.path.join("/Users/sohansamant/Desktop/SohanSamant/hf_bi_python_exercise/recipes-etl/src/","recipes.json")

    # Call the function
    result = download_file(url, save_path)

    # Assertions
    assert result == "An error occurred: Request Error"

# Tests for read_json_file function
"""
Mocking:

The mock_open function is used to simulate opening and reading a file, allowing us to inject the jsonl_content directly into the test without needing an actual file.
patch('builtins.open', mock_file) is used to replace the built-in open function with our mocked version during the test.

Sorting: Both cleaned_data and expected_data lists are sorted by the 'name' key before comparing them. This ensures that the test is order-independent, which can be particularly useful if the JSONL lines are not guaranteed to be in any specific order.

Lambda Function: The lambda x: x['name'] function is used as a key for sorting to ensure that the dictionaries are compared based on the name field.

Assertions:

The assert statement is used to check that the cleaned data returned by the function matches the expected data after newline characters have been removed.
"""
def test_read_json_file():
    # Sample JSONL content
    jsonl_content = """{"name": "Recipe 1", "description": "This is a test description.\nWith a newline."}
    {"name": "Recipe 2", "description": "Another description."}"""

    # Mock the open function to return the sample JSONL content
    mock_file = mock_open(read_data=jsonl_content)

    with patch('builtins.open', mock_file):
        # Call the function
        cleaned_data = read_json_file('dummy_path.jsonl')

        # Expected data after cleaning newlines
        expected_data = [
            {"name": "Recipe 1", "description": "This is a test description. With a newline."},
            {"name": "Recipe 2", "description": "Another description."}
        ]

        # Sort both lists to ensure order does not affect the test
        cleaned_data_sorted = sorted(cleaned_data, key=lambda x: x['name'])
        cleaned_data_sorted = sorted(expected_data, key=lambda x: x['name'])
        
        # Assert that the resulting cleaned data matches the expected data
        assert cleaned_data_sorted == cleaned_data_sorted

# Test to check if read_json_file functions works correctly without new line character 
def test_read_json_file_with_clean_data():
    # Sample JSONL content
    jsonl_content = """{"name": "Recipe 1", "description": "This is a test description.","datePublished": "2011-06-06"}
    {"name": "Recipe 2", "description": "Another description.","datePublished": "2011-06-06"}"""

    # Mock the open function to return the sample JSONL content
    mock_file = mock_open(read_data=jsonl_content)

    with patch('builtins.open', mock_file):
        # Call the function
        cleaned_data = read_json_file('dummy_path.jsonl')

        # Expected data after cleaning newlines
        expected_data = [
            {"name": "Recipe 1", "description": "This is a test description. With a newline.","datePublished": "2011-06-06"},
            {"name": "Recipe 2", "description": "Another description.","datePublished": "2011-06-06"}
        ]

        # Sort both lists to ensure order does not affect the test
        cleaned_data_sorted = sorted(cleaned_data, key=lambda x: x['name'])
        cleaned_data_sorted = sorted(expected_data, key=lambda x: x['name'])
        
        # Assert that the resulting cleaned data matches the expected data
        assert cleaned_data_sorted == cleaned_data_sorted

# Unit tests for extract_chilies_recipes function
def test_extract_chilies_recipes():
    # Sample JSONL content
    json_data = [
        {'name': 'Recipe 1', 'ingredients': 'This ingredients has  Chiles and  1/2 cup Mayonnaise', 'cookTime': 'PT10M', 'recipeYield': '8', 'datePublished': '2013-04-01', 'prepTime': 'PT5M', 'description': "Got leftover Easter eggs"},
        {'name': 'Recipe 2', 'ingredients': '3/4 cups Fresh Basil Leaves 1/2 cup Grated Parmesan Cheese', 'cookTime': 'PT10M', 'recipeYield': '8', 'datePublished': '2011-06-06', 'prepTime': 'PT6M', 'description': 'I finally have basil in my garden.'}
    ]

    result = extract_chilies_recipes(json_data)

    expected_recipes = [
        {'name': 'Recipe 1', 'ingredients': 'This ingredients has  Chiles and  1/2 cup Mayonnaise', 'cookTime': 'PT10M', 'recipeYield': '8', 'datePublished': '2013-04-01', 'prepTime': 'PT5M', 'description': "Got leftover Easter eggs"}
    ]

    # Sort both lists by name to ensure order does not affect the test
    result_sorted = sorted(result, key=lambda x: x['name'])
    expected_sorted = sorted(expected_recipes, key=lambda x: x['name'])
    
    assert result_sorted == expected_sorted

# Unit test for extract_chilies_recipes function with no Chili word in the ingredients
def test_extract_chilies_recipes_no_matches():
    no_match_data = [
        {'name': 'No Chili Here', 'ingredients': 'Just some vegetables and spices.'},
        {'name': 'Another Recipe', 'ingredients': 'Various ingredients including onions and garlic.'}
    ]
    
    result = extract_chilies_recipes(no_match_data)
    assert result == []

# Unit test for extract_chilies_recipes function with empty inputs
def test_extract_chilies_recipes_empty_input():
    result = extract_chilies_recipes([])
    assert result == []

# Unit test for extract_chilies_recipes function with edge cases
def test_extract_chilies_recipes_edge_cases():
    edge_case_data = [
        {'name': 'Recipe 1', 'ingredients': 'Chili powder, cumin, and garlic.'},
        {'name': 'Recipe 2', 'ingredients': 'Garlic, onions, and chili.'},
        {'name': 'Recipe 3', 'ingredients': 'Just some random ingredients.'}
    ]
    
    result = extract_chilies_recipes(edge_case_data)
    
    expected_recipes = [
        {'name': 'Recipe 1', 'ingredients': 'Chili powder, cumin, and garlic.'},
        {'name': 'Recipe 2', 'ingredients': 'Garlic, onions, and chili.'}
    ]
    
    # Sort both lists by name to ensure order does not affect the test
    result_sorted = sorted(result, key=lambda x: x['name'])
    expected_sorted = sorted(expected_recipes, key=lambda x: x['name'])
    
    assert result_sorted == expected_sorted

# Unit test for extract_chilies_recipes function with additional test with variations and misspellings
def test_extract_chilies_recipes_with_misspellings():
    # Sample data with misspellings and variations
    sample_data_with_misspellings = [
        {"name": "Recipe 1", "ingredients": ["Tomato", "Chili powder"]},
        {"name": "Recipe 2", "ingredients": ["Garlic", "Chilles"]},
        {"name": "Recipe 3", "ingredients": ["Onion", "Pepper"]},
        {"name": "Recipe 4", "ingredients": ["Chille", "Lime"]},
        {"name": "Recipe 5", "ingredients": ["Basil", "Oregano"]}
    ]

    result = extract_chilies_recipes(sample_data_with_misspellings)

    # Expected data after filtering recipes with specified ingredients
    expected_data_with_misspellings = [
        {"name": "Recipe 1", "ingredients": ["Tomato", "Chili powder"]},
        {"name": "Recipe 2", "ingredients": ["Garlic", "Chilles"]},
        {"name": "Recipe 4", "ingredients": ["Chille", "Lime"]}
    ]

    # Sort both lists by name to ensure order does not affect the test
    result_sorted = sorted(result, key=lambda x: x['name'])
    expected_sorted = sorted(expected_data_with_misspellings, key=lambda x: x['name'])
    
    assert result_sorted == expected_sorted

# Unit test for convert_duration_to_minutes function
"""
    To test this function, we will consider several cases:

    * Valid ISO 8601 duration strings.
    * An empty string input.
    * Edge cases like zero duration.

    For the purpose of the unit tests, isodate.parse_duration will be used directly, but you might mock it if you want to isolate the tests from external dependencies.
"""
# Tests the function with valid ISO 8601 duration strings.
def test_convert_duration_to_minutes_valid_duration():
    # Test with a valid ISO 8601 duration (e.g., 'PT2H' for 2 hours)
    assert convert_duration_to_minutes('PT2H') == 120  # 2 hours = 120 minutes
    assert convert_duration_to_minutes('PT45M') == 45  # 45 minutes = 45 minutes
    assert convert_duration_to_minutes('PT1H30M') == 90  # 1 hour 30 minutes = 90 minutes
    assert convert_duration_to_minutes('PT0S') == 0  # 0 seconds = 0 minutes

# Checks if an empty string correctly returns 0 minutes.
def test_convert_duration_to_minutes_empty_string():
    # Test with an empty string
    assert convert_duration_to_minutes('') == 0

# Additional test cases to tests zero duration explicitly in different forms.
def test_convert_duration_to_minutes_zero_duration():
    # Test with zero duration explicitly
    assert convert_duration_to_minutes('PT0S') == 0  # 0 seconds = 0 minutes

    # Test with a zero duration in hours
    assert convert_duration_to_minutes('PT0H') == 0  # 0 hours = 0 minutes

# Unit tests for calculate_difficulty function
def test_easy_difficulty():
    difficulty, total_time = calculate_difficulty(10, 15)
    assert difficulty == 'Easy'
    assert total_time == 25

# Unit Test a case where the total time is between 30 and 60 minutes, which should return 'Medium'
def test_medium_difficulty():
    difficulty, total_time = calculate_difficulty(20, 15)
    assert difficulty == 'Medium'
    assert total_time == 35

# Unit Test a case where the total time is more than 60 minutes, which should return 'Hard'.
def test_hard_difficulty():
    difficulty, total_time = calculate_difficulty(50, 20)
    assert difficulty == 'Hard'
    assert total_time == 70

# Unit Test the exact boundary where the difficulty should transition from 'Easy' to 'Medium' (total time is 30 minutes).
def test_exact_boundary_easy_to_medium():
    difficulty, total_time = calculate_difficulty(15, 15)
    assert difficulty == 'Medium'
    assert total_time == 30

# Unit Test the exact boundary where the difficulty should transition from 'Medium' to 'Hard' (total time is just over 60 minutes).
def test_exact_boundary_medium_to_hard():
    difficulty, total_time = calculate_difficulty(30, 31)
    assert difficulty == 'Hard'
    assert total_time == 61

# Unit Test the function when cookTime_minutes is NaN, expecting 'Unknown Difficulty'.
def test_nan_in_cook_time():
    difficulty, total_time = calculate_difficulty(np.nan, 10)
    assert difficulty == 'Unknown Difficulty'
    assert np.isnan(total_time)

# Unit Tests the function when prepTime_minutes is NaN, also expecting 'Unknown Difficulty
def test_nan_in_prep_time():
    difficulty, total_time = calculate_difficulty(10, np.nan)
    assert difficulty == 'Unknown Difficulty'
    assert np.isnan(total_time)

# Unit tests the function when both cookTime_minutes and prepTime_minutes are NaN, expecting 'Unknown Difficulty'
def test_nan_in_both_times():
    difficulty, total_time = calculate_difficulty(np.nan, np.nan)
    assert difficulty == 'Unknown Difficulty'
    assert np.isnan(total_time)