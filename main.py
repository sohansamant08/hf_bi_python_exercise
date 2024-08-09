import json
import os
import re
import requests
import isodate
import pandas as pd
import numpy as np
import datetime

# Function to read the config file
"""
    :param configFile
    :return: config object
"""
def read_configs( configFile ):
    with open(configFile, 'r') as file:
        config = json.load(file)
        return config

# Function to download the file
"""
    :param sourceFileUrl
    :param saveFilePath
    :return: file
"""
def download_file(sourceFileUrl, saveFilePath):
    try:
        # Send a GET request to the URL
        response = requests.get(sourceFileUrl, stream=True)

        # Check if the request was successful
        response.raise_for_status()
        
        # Open the file in binary write mode and save the content
        with open(saveFilePath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
            #file.write(response.content)
        
        return f"File downloaded successfully and saved to {saveFilePath}"
    
    except requests.exceptions.HTTPError as http_err:
        return f"HTTP error occurred: {http_err}"
    except requests.exceptions.ConnectionError as conn_err:
        return f"Connection error occurred: {conn_err}"
    except requests.exceptions.Timeout as timeout_err:
        return f"Timeout error occurred: {timeout_err}"
    except requests.exceptions.RequestException as req_err:
        return f"An error occurred: {req_err}"
    except Exception as err:
        return f"An unexpected error occurred: {err}"

# Function to read jsonl File
"""
    :param filePath
    :return: Array of json object
"""
def read_json_file(filePath):
    data = []
    with open(filePath, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                # Parse each line as a JSON object
                json_obj = json.loads(line.strip())

                # Remove new line characters from all string fields
                cleaned_JsonObj = {key: (value.replace('\n', ' ') if isinstance(value, str) else value)
                            for key, value in json_obj.items()}

                data.append(cleaned_JsonObj)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {line.strip()}")
                print(f"Error: {e}")
    return data

# Function to extract recipes with "Chilies" or its variants
# Regular expression pattern to match "Chilies", "Chiles", "Chili", "Chile" (case insensitive)
"""
    :param jsonData
    Explanation for Regex:
    \b: Word boundary ensures that the match occurs at the start and end of a word, so it doesn't match within other words.
    Chil: Matches the common prefix "Chil" in all the words.
    (l|e)?: Matches an optional "l" or "e." This handles variations like "Chile" and "Chille."
    (i|e)?: Matches an optional "i" or "e." This handles variations like "Chilli" and "Chille."
    s?: Matches an optional "s." This handles the plural forms like "Chiles" and "Chilles."
    \b: Another word boundary to ensure the match ends at the end of the word.
"""
def extract_chilies_recipes(jsonData):
    chilies_recipes = []
    pattern = re.compile(r'\bChil(l|e)?(i|e)?s?\b', re.IGNORECASE)

    for recipe in jsonData:
        ingredients = recipe.get('ingredients', []) 
        # Ensure ingredients are processed as a list of strings
        if isinstance(ingredients, str):
            # Search directly within the ingredients string
            if pattern.search(ingredients):
                chilies_recipes.append(recipe)
        elif isinstance(ingredients, list):
            # Check each ingredient if ingredients is a list
            if any(pattern.search(str(ingredient)) for ingredient in ingredients if isinstance(ingredient, str)):
                chilies_recipes.append(recipe)
    return chilies_recipes

# Function to converts PT duration to minutes 
"""
    :param pt_duration
    :return: duration in minutes
"""
def convert_duration_to_minutes(pt_duration):
    if pt_duration == '':
        return 0
    try:
        duration = isodate.parse_duration(pt_duration)
        seconds = duration.total_seconds()
    except (ValueError, isodate.ISO8601Error) as e:
        # Handle invalid duration input
        raise ValueError("Invalid duration format") from e
    except Exception as e:
        # Catch any other exceptions
        raise ValueError("An unexpected error occurred") from e
    minutes = seconds / 60
    return minutes

# The function calculates difficulty of the cooking process
"""
    :param cook_time: int
    :param prep_time: int
    :return: difficulty: string
"""
def calculate_difficulty(cookTime_minutes, prepTime_minutes):
    total_time = cookTime_minutes + prepTime_minutes
    if np.isnan(total_time):
        return 'Unknown Difficulty',total_time
    elif total_time > 60:
        return 'Hard',total_time
    elif total_time >= 30:
        return 'Medium',total_time
    else:
        return 'Easy',total_time

def main():
    print("** Starting ETL process for Hello Fresh ** ",datetime.datetime.now())
    # read config file
    config = read_configs("/Users/sohansamant/Desktop/SohanSamant/hf_bi_python_exercise/recipes-etl/src/config.json")

    # URL of the file to download
    sourceFileUrl = config.get('sourceFileUrl')
    # Path where you want to save the downloaded file
    saveFilePath = config.get('saveFilePath')
    # output path
    chileOutputFilePath = config.get('chileOutputFilePath')
    # Results path
    resultOutputFilePath = config.get('resultOutputFilePath')

    # Check if file already exist, download only if it doesn't exist
    if os.path.isfile(saveFilePath):
        print(f"The file '{saveFilePath}' already exists. No download needed.")
    else:
        # Call the function to download the file
        download_file(sourceFileUrl, saveFilePath)
    
    # Read file in dataframe and perform ETL
    jsonData = read_json_file(saveFilePath) # This is returned as an array of objects

    # Extract recipes with "Chilies" or its variants
    recipes_with_chilies = extract_chilies_recipes(jsonData)

    # Convert to Dataframe
    recipes_with_chiliesDF = pd.DataFrame(recipes_with_chilies)

    # Proceeding further only if there is any data with recipes that has “Chilies” as one of the ingredients.
    if not recipes_with_chiliesDF.empty:

        # Parse durations and convert ISO time to minutes
        recipes_with_chiliesDF['cookTime_minutes'] = recipes_with_chiliesDF['cookTime'].apply(convert_duration_to_minutes)
        recipes_with_chiliesDF['prepTime_minutes'] = recipes_with_chiliesDF['prepTime'].apply(convert_duration_to_minutes)

        # Calculate difficulty: apply(lambda row) apply function to each row of dataframe.
        # We can use the apply() function to apply the lambda function to both rows and columns of a dataframe. 
        # If the axis argument in the apply() function is 0, then the lambda function gets applied to each column, and if 1, then the function gets applied to each row.
        recipes_with_chiliesDF['difficulty'], recipes_with_chiliesDF['total_time']= zip(*recipes_with_chiliesDF.apply(lambda row: calculate_difficulty(row['cookTime_minutes'], row['prepTime_minutes']),axis=1))

        # Get average of total_time grouped by difficulty level
        # First filtering out unknown difficulty level
        recipes_with_chiliesDF=recipes_with_chiliesDF[recipes_with_chiliesDF['difficulty'] != 'Unknown Difficulty']

        # Group by difficulty and calculate the average total_time
        average_total_time_by_difficulty = recipes_with_chiliesDF.groupby('difficulty')['total_time'].mean().reset_index()
        average_total_time_by_difficulty['remark'] = 'average_total_time'

        # Reorder the columns
        results_DF = average_total_time_by_difficulty[['difficulty', 'remark', 'total_time']]
        # Write the results to a CSV file with a '|' separator
        results_DF.to_csv(resultOutputFilePath, sep='|', index=False, header=False)

        # Save dataframe to csv
        recipes_with_chiliesDF = recipes_with_chiliesDF.drop(columns=['prepTime_minutes', 'cookTime_minutes', 'total_time']).drop_duplicates()
        # Write Chiles data to csv
        recipes_with_chiliesDF.to_csv(chileOutputFilePath, sep='|',index=False)

if __name__ == "__main__":
    main()