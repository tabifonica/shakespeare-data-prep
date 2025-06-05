"""Data processing functions."""

import pandas as pd
import re
import json

def extract_hamlet(df):
    """Exracts Hamlet from the given DataFrame."""
    # Filter the DataFrame for the play "Hamlet"
    hamlet_rows = []
    for _, row in df.iterrows():
        if row["Play"] == "Hamlet":
            hamlet_rows.append(row)
    hamlet_df = pd.DataFrame(hamlet_rows).reset_index(drop=True)

    return hamlet_df

def process_act_scene_line(df):
    """
    Processes the ActSceneLine column to extract Act, Scene, and Line numbers.
    The format is expected to be 'Act.Scene.Line'.
    If ActSceneLine is None, it looks for the next non-None value.
    """
    acts = []
    scenes = []
    lines = []

    for i, row in df.iterrows():
        # If ActSceneLine is None, look for the next non-None value
        if row["ActSceneLine"] is None or pd.isna(row["ActSceneLine"]):
            next_valid = df["ActSceneLine"].iloc[i+1:].dropna()
            if not next_valid.empty:
                act_scene_line = str(next_valid.iloc[0]).split(".")
            else:
                act_scene_line = ["0", "0", "0"]  # Default values if no next row is found

        else:
            act_scene_line = str(row["ActSceneLine"]).split(".")

        acts.append(act_scene_line[0])
        scenes.append(act_scene_line[1])
        lines.append(act_scene_line[2])

    df["Act"] = acts
    df["Scene"] = scenes
    df["Line"] = lines

    return df

def extract_names(text: str):
    """
    Extracts a list of names from a given text string,
    for use in the 'Characters Present' field.
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

def process_stage_directions(df):
    """
    Processes the dataframe to add a characters present column.
    """
    stage_directions = []
    new_lines = []
    new_lines_with_sd = []
    characters = set()
    current_play = None
    current_act = None
    current_scene = None
    current_speaker = None
    for _, row in df.iterrows():

        # Reset characters if the play, act, or scene changes
        if (row["Play"] != current_play or
            row["Act"] != str(current_act) or
            row["Scene"] != str(current_scene)):

            characters = set()
            current_play = row["Play"]
            current_act = int(row["Act"])
            current_scene = int(row["Scene"])

        # Process Stage Directions
        if row["ActSceneLine"] is None or pd.isna(row["ActSceneLine"]):
            sd = row["PlayerLine"] # sd stands for "Stage Direction"

            # Drop unnecessary columns
            sd_row = row.drop(["Player", "PlayerLinenumber",
                                "ActSceneLine"]).to_dict()

            # extract names
            names = extract_names(sd)

            if "Enter" in sd or "Re-enter" in sd:
                sd_row["StageDirection"] = "Enter"
                characters.update(names)

            elif "Exit" in sd:
                sd_row["StageDirection"] = "Exit"

                # if there's an exit with no names, assume the speaker is exiting
                if not names or names == [""]:
                    names = [current_speaker]

                characters.difference_update(names)

            elif "Exeunt all but" in sd:
                sd_row["StageDirection"] = "Exuent all but"

                # assume the names are the ones remaining
                characters = set(names)

            elif "Exeunt" in sd:
                sd_row["StageDirection"] = "Exeunt"

                # if there's an exit with no names, assume all are exiting
                if not names or names == [""]:
                    names = list(characters)

                characters.difference_update(names)

            else:
                sd_row["StageDirection"] = "Other"

            sd_row["Characters"] = names
            stage_directions.append(sd_row)
            new_lines_with_sd.append(sd_row)

        # Add character columns to the rest of the lines
        else:
            # Reset current speaker
            current_speaker = row["Player"]

        new_line = row.copy()
        new_line["Characters"] = list(characters)
        new_lines.append(new_line)
        new_lines_with_sd.append(new_line)

    stage_directions = pd.DataFrame(stage_directions)
    new_df = pd.DataFrame(new_lines)
    new_lines_with_sd = pd.DataFrame(new_lines_with_sd)

    return new_df, stage_directions, new_lines_with_sd

def extract_unique_players(df):
    """
    Returns a list of JSON objects, each with 'Play' and 'Players' fields.
    'Play' is the play name, 'Players' is a sorted list of unique players in that play.
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

def chunk(df, chunk_size=150):
    """
    Chunks a JSON by the number of words in the PlayerLine field.
    """

    chunks = []
    current_chunk = {}
    current_word_count = 0

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        row_word_count = len(row["PlayerLine"].split())

        # if the chunk is empty, initialize it with the current row
        if not current_chunk:
            current_chunk = row_dict
            current_word_count = row_word_count

        # If adding this row would exceed the chunk size,
        # OR the play, act, or scene changes,
        # save the current chunk and start a new one
        elif (current_word_count + row_word_count > chunk_size
              or current_chunk["Play"] != row["Play"]
              or current_chunk["Act"] != row["Act"]
              or current_chunk["Scene"] != row["Scene"]):

            # If the current chunk is from the old format, merge it with the previous chunk
            if "firstLine" not in current_chunk:
                #print("Current chunk:", json.dumps(current_chunk, indent=2))
                #print("Previous chunk:", json.dumps(chunks[-1], indent=2))
                #print("Current row:", json.dumps(row_dict, indent=2))
                #chunks[-1] = merge_chunks([chunks[-1], current_chunk])
                #current_chunk = row_dict
                #current_word_count = row_word_count
                chunks.append(merge_chunks([row_dict]))
            else:
                chunks.append(current_chunk)

            current_chunk = {}
            current_word_count = 0

        # Otherwise, merge the current row into the current chunk
        else:
            current_chunk = merge_chunks([current_chunk, row_dict])
            current_word_count += row_word_count

    # remove "Speaker Order" from all the chunks
    for c in chunks:
        c.pop("SpeakerOrder", None)

    return chunks

def merge_chunks(chunks):
    """
    Merges a list of JSON objects (dicts) into a single JSON object.
    For string fields, concatenates text. For list fields, appends lists.
    For other fields, keeps the last value.
    """
    merged = {}
    text = ""
    speakers = set()
    speaker_order = []
    characters_present = set()
    play = chunks[0]["Play"]
    act = chunks[0]["Act"]
    scene = chunks[0]["Scene"]

    for c in chunks:
        # throw an error if the play, act, or scene changes
        if (c["Play"] != play or
            c["Act"] != act or
            c["Scene"] != scene):
            raise ValueError("Cannot merge chunks from different plays, acts, or scenes. Chunks:",
                             json.dumps(chunks[0], indent=2), json.dumps(chunks[1], indent=2))

        # If chunk has a Player field, add it to the speakers
        if c.get("Player") and isinstance(c["Player"], str):
            speakers.add(c["Player"])

            if c.get("Characters"):
                characters_present.update(c["Characters"])

            # Add the speaker's name to the text if the speaker changes
            try:
                if not speaker_order or speaker_order[-1] != c["Player"]:
                    speaker_order.append(c["Player"])
                    c["PlayerLine"] = "\n" + c["Player"] + ": " + c["PlayerLine"]
            except TypeError as exc:
                raise TypeError("Error processing chunk: " + json.dumps(c, indent=2)) from exc

        # If chunk has a Speakers field, add them to the speakers
        elif c.get("Speakers"):
            speakers.update(c["Speakers"])
            speaker_order.extend(c["SpeakerOrder"])

            if c.get("CharactersPresent"):
                characters_present.update(c["CharactersPresent"])

        # Update the text with the PlayerLine field
        if not text:
            text = c["PlayerLine"]
        else:
            text += "\n" + c["PlayerLine"]

    merged = chunks[0].copy()
    merged["PlayerLine"] = text
    merged["Speakers"] = list(speakers)
    merged["SpeakerOrder"] = speaker_order

    if merged.get("Line"):
        merged["firstLine"] = chunks[0]["Line"]

    merged["lastLine"] = chunks[-1]["Line"]

    if characters_present:
        merged["CharactersPresent"] = list(characters_present)

    for key in ["PlayerLinenumber", "Player", "Characters", "Line", "ActSceneLine", "Dataline"]:
        merged.pop(key, None)

    return merged
