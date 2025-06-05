Breaks Shakespeare's plays into chunks for use in Retrieval Augmented Generation (RAG)
for a generative language model.
Also processes metadata for each chunk,
including Act, Scene, firstLine, lastLine, Speakers and CharactersPresent.

**Language model with RAG repository:** https://github.com/tabifonica/shakespeare-data-prep

Input format: a CSV file of Shakespeare's plays "shakespeare.csv" with the following columns:
    "Dataline" - an index
    "Play" - the name of the play
    "PlayerLinenumber" - the line number of the player,
        eg. if the player is Hamlet, and PlayerLineNumber is 44, that is Hamlet's 44th line
    "ActSceneLine" - the act, scene and line number in the format "Act.Scene.Line"
    "Player" - the speaker of the line
    "PlayerLine" - the number of the line

Output: a JSON file "shakespeare_chunked.json" with a list of JSON objects in the following format:
    {
        "Play": "Hamlet",
        "PlayerLine": Enter HAMLET\n\nHAMLET: To be, or not to be: that is the question: [etc.]
        "Act": "3",
        "Scene": "1",
        "Speakers": [
            "HAMLET",
            "LORD POLONIUS"
        ],
        "firstLine": "63",
        "lastLine": "79",
        "CharactersPresent": [
            "HAMLET",
            "POLONIUS",
            "KING CLAUDIUS",
            "OPHELIA"
        ],
    },

# Code summary
This repository contains three python files:
- main.py - the main function
- data.py - the parimary data processing functions
- utils.py - utility functions

Three primary data processing functions are performed on the data.
1. process_act_scene_line - This takes the "ActSceneLine" column in the CSV and turns it into three separate columns.
                            Lines that don't have ActSceneLine (stage direcitons) are given the next available values,
                            so that every row has Act, Scene and Line values.
2. process_stage_directions - This uses stage directions to get a list of characters on set at any given time in the play.
                            In the final JSON output, this is the "CharactersPresent" field.
3. chunk - This groups lines together into chunks so that they can be used by a Retrieval Augmented Generation (RAG) system.
                            Chunks are 150 words by default and cannot span across Act/Scene/Line boundaries.

# How to run this code
1. Make an activate a python virtual environment: python3 -m venv venv
2. Activate the virtual environment (the following code is for Mac): source venv/bin/activate
3. Install requirements: python3 -r requirements.txt
4. Run: python3 main.py
