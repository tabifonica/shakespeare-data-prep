"""
Breaks Shakespeare's plays into chunks for use in Retrieval Augmented Generation (RAG).
Also processes metadata for each chunk,
including Act, Scene, firstLine, lastLine, Speakers and CharactersPresent.
See the readme file for more details.
"""
import json
import argparse
import pandas as pd

import data # data processing functions from data.py

def prepare_shakespeare_data(input_csv="shakespeare.csv", output_json="shakespeare_chunked.json"):
    """
    Input: CSV file with Shakespeare's plays (see readme for full specs)
    Output: JSON file with chunked data.
    """
    # Read and print the CSV file
    df = pd.read_csv(input_csv)

    # Split the 'Act', 'Scene', and 'Line' columns into separate columns
    shakespeare = data.process_act_scene_line(df)

    # Process stage directions into a 'CharactersPresent' column
    shakespeare_aug = data.process_stage_directions(shakespeare)

    # Chunk lines
    shakespeare_chunked = data.chunk(shakespeare_aug)

    # Convert chunked data to json format
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(shakespeare_chunked, f, indent=2)

if __name__ == "__main__":
    # Allow user to specify input and output file paths
    parser = argparse.ArgumentParser(description="Process and chunk Shakespeare CSV data.")
    parser.add_argument("--input_csv", type=str,
        default="shakespeare.csv",
        help="Path to input CSV file"
        )
    parser.add_argument("--output_json", type=str,
        default="shakespeare_chunked.json",
        help="Path to output JSON file"
        )
    args = parser.parse_args()

    prepare_shakespeare_data(args.input_csv, args.output_json)
