"""Data processing functions."""
import re
import json
import pandas as pd

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

def process_act_scene_line(df):
    """
    Processes the ActSceneLine column to extract Act, Scene, and Line numbers.
    The format is expected to be 'Act.Scene.Line'.
    If ActSceneLine is None, it looks for the next non-None value.
    Input: DataFrame of Shakespeare's plays
    Output: Same DataFrame with additional 'Act', 'Scene', and 'Line' columns.
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

def process_stage_directions(df):
    """
    Processes the dataframe to add a characters present column.
    Input: Dataframe of Shakespeare's plays
    Output: Same dataframe with an additional 'Characters' column
    """
    new_lines = []
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

            # extract names
            names = extract_names(sd)

            # If characters enter the scene, add them to the characters set
            if "Enter" in sd or "Re-enter" in sd:
                characters.update(names)

            # If characters exit the scene, remove them from the characters set
            elif "Exit" in sd:
                # if there's an exit with no names, assume the speaker is exiting
                if not names or names == [""]:
                    names = [current_speaker]

                characters.difference_update(names)

            elif "Exeunt all but" in sd:
                # assume the names are the ones remaining
                characters = set(names)

            # 'exuent' is used for multiple characters exiting
            elif "Exeunt" in sd:
                # if there's an exit with no names, assume all are exiting
                if not names or names == [""]:
                    names = list(characters)

                characters.difference_update(names)

        else:
            # Reset current speaker
            current_speaker = row["Player"]

        # Add character columns to the dataframe
        new_line = row.copy()
        new_line["Characters"] = list(characters)
        new_lines.append(new_line)

    new_df = pd.DataFrame(new_lines)

    return new_df

def extract_unique_players(df):
    """
    Maps character names to plays
    Input: DataFrame of Shakespeare's plays
    Output: List of dictionaries with unique players for each play
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
    Input: DataFrame of Shakespeare's plays
    Output: List of JSON objects (dicts) representing chunks of the play.
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

            # Add the current chunk to the list of chunks
            chunks.append(current_chunk)

            # Reset the current chunk with the new row
            current_chunk = merge_chunks([row_dict])
            current_word_count = row_word_count

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
    Merges a list of JSON objects (dicts) into a single JSON object,
    and formats it into the new format.
    Input: List of JSON objects (dicts) 
        (normally just two, but one chunk can also be passed to convert it to the new format)
    Output: Merged JSON object (dict) in the new format

    Output format:
    {
        "Play": "Hamlet",
        "PlayerLine": "\nHAMLET: Whether 'tis nobler in the mind to suffer
        \nThe slings and arrows of outrageous fortune,
        \nOr to take arms against a sea of troubles,
        \nAnd by opposing end them? To die: to sleep,
        \nNo more, and by a sleep to say we end
        \nThe heart-ache and the thousand natural shocks
        \nThat flesh is heir to, 'tis a consummation
        \nDevoutly to be wish'd. To die, to sleep,
        \nTo sleep: perchance to dream: ay, there's the rub,
        \nFor in that sleep of death what dreams may come
        \nWhen we have shuffled off this mortal coil,
        \nMust give us pause: there's the respect
        \nThat makes calamity of so long life,
        \nFor who would bear the whips and scorns of time,
        \nThe oppressor's wrong, the proud man's contumely,
        \nThe pangs of despised love, the law's delay,
        \nThe insolence of office and the spurns
        \nThat patient merit of the unworthy takes,",
        "Act": "3",
        "Scene": "1",
        "Speakers": [
            "HAMLET"
        ],
        "firstLine": "65",
        "lastLine": "82",
        "CharactersPresent": [
            "HAMLET",
            "OPHELIA"
        ]
    },
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

    # Process the merged chunk format
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
