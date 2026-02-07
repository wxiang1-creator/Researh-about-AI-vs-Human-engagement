# Copilot Instructions for AI Coding Agents

## Overview
This project focuses on downloading and processing Reddit comments using the `datasets` library. The main script is `01_hf_download.py`, which handles the data retrieval and storage.

## Architecture
- **Main Components**: The primary component is the `01_hf_download.py` script, which interacts with the `datasets` library to fetch data from Reddit.
- **Data Flow**: The script downloads the dataset, converts it to a pandas DataFrame, and saves it as a CSV file in the `data/raw` directory.
- **Why**: This structure allows for easy data manipulation and analysis in subsequent scripts.

## Developer Workflows
- **Running the Download**: Execute `01_hf_download.py` to download the dataset. Ensure that the required packages are installed as specified in `requirements.txt`.
- **Testing**: Currently, there are no explicit tests defined. Consider implementing tests for data integrity after download.
- **Debugging**: Use print statements to debug the data retrieval process. Check the output CSV for expected columns and data.

## Project-Specific Conventions
- **Data Storage**: Raw data is stored in `data/raw/`. Ensure that any new data downloads follow this structure.
- **File Naming**: Use descriptive names for output files to indicate their content and source.

## Integration Points
- **External Dependencies**: The project relies on the following libraries:
  - `datasets`: For loading datasets from external sources.
  - `pandas`: For data manipulation and storage.
  - `pyarrow`: Required for efficient data handling.
  - `matplotlib` and `tqdm`: For visualization and progress tracking, respectively.

## Communication Patterns
- The script currently operates independently. Future scripts should consider how to share data between them, possibly through shared functions or classes.

## Example Usage
To run the download script:
```bash
python src/01_hf_download.py
```

## Conclusion
This document serves as a guide for AI agents to understand the project structure and workflows. For further assistance, refer to the code comments and the `requirements.txt` for dependencies.