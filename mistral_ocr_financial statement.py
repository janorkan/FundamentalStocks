# Analyze Financial statements with OCR
import os
import re
import time
import datauri
from pathlib import Path
from mistral_connect import mistral_con


# Extract year and quarter from parthname (not used due to regex complexity for different companies)
def extract_year_quarter(filename):
    mistral = mistral_con()
    model = "mistral-large-latest"
    prompt = f"Please extract the Year and the quarter from the filename: {filename}. Please return the result in the format: Year:, Quarter: and numbers only."
    chat_response = mistral.chat.complete(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt,
            },
        ],
    )
    response = chat_response.choices[0].message.content
    year_match = re.search(r"Year:\s*(\d{4})", response)
    quarter_match = re.search(r"Quarter:\s*(\d+)", response)
    fy_match = re.search(r"FY[-_](\d{2})", filename, re.IGNORECASE)

    if year_match and quarter_match:
        year = int(year_match.group(1))
        quarter = int(quarter_match.group(1))
        return year, quarter

    if year_match and fy_match:
        year = int(year_match.group(1))
        return year, 5  # FY is considered the highest priority

    return None, None, response


# Get newest file from path (not used, changed to url input)
def get_newest_file(symbol):
    base_path = "./statements"
    matching_folders = [
        os.path.join(base_path, folder)
        for folder in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, folder))
        and folder.endswith(f"- {symbol}")
    ]

    if not matching_folders:
        raise FileNotFoundError(f"No folder found with symbol: {symbol}")

    target_folder = matching_folders[0]

    files = [
        os.path.join(target_folder, f)
        for f in os.listdir(target_folder)
        if os.path.isfile(os.path.join(target_folder, f))
    ]
    if not files:
        raise FileNotFoundError(f"No files found in folder: {target_folder}")

    newest_file = None
    newest_year_quarter = (0, 0)

    for file in files:
        time.sleep(2)  # Sleep timer of 2 seconds
        year_quarter = extract_year_quarter(os.path.basename(file))
        if year_quarter > newest_year_quarter:
            newest_year_quarter = year_quarter
            newest_file = file

    return newest_file, newest_year_quarter


# Mistral OCR online path
def mistral_ocr_url(url, symbol, year, quarter):
    # Create filename and folder to save md_file
    filepath = f"./statements/{symbol}/"
    md_filename = f"{year}-Q{quarter} Financial statement {symbol}.md"
    Path(filepath).mkdir(parents=True, exist_ok=True)

    # Check if file already exists
    my_file = Path(filepath, md_filename)
    if my_file.is_file():
        return print(f"File {md_filename} already exists! No OCR executed"), Path(
            filepath, md_filename
        )
    else:
        # Mistral OCR request
        mistral = mistral_con()
        ocr_response = mistral.ocr.process(
            model="mistral-ocr-latest",
            document={
                "type": "document_url",
                "document_url": url,
            },
            include_image_base64=True,
        )

        # save md_file with all images
        with open(Path(filepath, md_filename), "wt") as f:
            for page in ocr_response.pages:
                f.write(page.markdown)
                for image in page.images:
                    parsed = datauri.parse(image.image_base64)
                    with open(Path(filepath, image.id), "wb") as file:
                        file.write(parsed.data)
        return md_filename, ocr_response


if __name__ == "__main__":
    symbol = "PYPL"
    # filepath = get_newest_file(symbol)
    # print(filepath)

    url = "https://s205.q4cdn.com/875401827/files/doc_financials/2024/q4/PYPL-4Q-24-Earnings-Release.pdf"
    year = 2024
    quarter = 4
    filename = (
        f"./statements/{symbol}/{year}-Q{quarter} Financial statement {symbol}.md"
    )

    md_filename, ocr_response = mistral_ocr_url(url, symbol, year, quarter)
    print(md_filename)
    # print(ocr_response)
