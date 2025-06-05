"""Utility functions for processing Shakespeare's plays."""
import re
import pandas as pd

def extract_names(text: str):
    """
    Extracts a list of names from a given text string,
    for use in the 'Characters Present' field.
    Input: text string
    Output: list of names
    """
    names = re.split(r",? and |, |\. ", text)

    # Clean up names
    cleaned_names = []
    for name in names:
        if name is None or name.strip() == "":
            continue

        # remove stage directions
        if "Enter" in name or "Exit" in name or "Exeunt" in name or "Re-enter" in name:
            name = name.replace(
                "Enter", "").replace(
                    "Exit", "").replace(
                        "Exeunt", "").replace(
                            "Re-enter", "").strip()

        # remove if all lowercase
        elif name.islower():
            continue

        # remove any lowercase words from names
        words = [word for word in name.split(" ") if not word.islower()]
        name = " ".join(words)

        cleaned_names.append(name.strip())

    return cleaned_names

def extract_hamlet(df):
    """
    Exracts Hamlet from the given DataFrame.
    This is used for testing purposes.
    """
    # Filter the DataFrame for the play "Hamlet"
    hamlet_rows = []
    for _, row in df.iterrows():
        if row["Play"] == "Hamlet":
            hamlet_rows.append(row)
    hamlet_df = pd.DataFrame(hamlet_rows).reset_index(drop=True)

    return hamlet_df

def map_characters_to_play(df):
    """
    Maps character names to plays
    Input: DataFrame of Shakespeare's plays
    Output: List of dictionaries with unique players for each play

    This function is not used in the final program.
    """
    plays = df["Play"].unique()
    result = []
    for play in plays:

        play_df = []
        for _, row in df.iterrows():
            if row["Play"] == play:
                play_df.append(row)
        play_df = pd.DataFrame(play_df)

        players = set()
        for _, row in play_df.iterrows():
            if (row["Player"] is not None and not pd.isna(row["Player"])
                and row["Player"].strip() != ""
                and row["Player"].isupper()):
                players.add(row["Player"].strip())
        result.append({
            "Play": play,
            "Players": sorted(players)
        })
    return result
