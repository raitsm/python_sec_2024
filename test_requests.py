# test module to see file retrieval from an url.

import os
import requests
import tarfile

def text_file_download(source_url, save_folder, file_name):
    """
    Downloads a CSV file from a public GitHub repository and saves it to a specified folder.

    :param github_url: str, URL to the raw CSV file on GitHub.
    :param save_folder: str, folder where the CSV file will be saved.
    :param file_name: str, name of the file to save locally.
    """
    # Ensure the save folder exists
    os.makedirs(save_folder, exist_ok=True)

    # Full path to save the file
    save_path = os.path.join(save_folder, file_name)

    try:
        # Stream the download for large files
        with requests.get(source_url, stream=True) as response:
            response.raise_for_status()  # Raise an error for bad HTTP responses
            with open(save_path, "wb") as file:
                print(f"Downloading {file_name}...")
                # Write the file in chunks to handle large sizes
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                    file.write(chunk)
        print(f"Download completed. File saved to: {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")


def download_binary_file(url, save_folder, file_name):
    """
    Downloads a binary file from a URL and saves it to a specified folder.
    
    :param url: str, URL of the file to download.
    :param save_folder: str, folder where the file will be saved.
    :param file_name: str, name of the file to save locally.
    """
    # Ensure the save folder exists
    os.makedirs(save_folder, exist_ok=True)
    
    # Full path to save the file
    save_path = os.path.join(save_folder, file_name)
    
    try:
        # Stream the download for efficiency
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Raise an error for bad HTTP responses
            with open(save_path, "wb") as file:
                print(f"Downloading {file_name}...")
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                    file.write(chunk)
        print(f"Download completed. File saved to: {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        

def extract_tar_gz(file_path, extract_to):
    """
    Extracts a .tar.gz file to a specified directory.
    
    :param file_path: str, path to the .tar.gz file.
    :param extract_to: str, directory to extract the contents into.
    """
    try:
        with tarfile.open(file_path, "r:gz") as tar:
            print(f"Extracting {file_path} to {extract_to}...")
            tar.extractall(path=extract_to)
        print("Extraction completed.")
    except Exception as e:
        print(f"An error occurred during extraction: {e}")


# Example usage
if __name__ == "__main__":
    # Replace with the raw GitHub URL of your CSV file
    source_url = "https://github.com/logpai/loghub/tree/master/OpenSSH"

    source_url = "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1"
    
    
    # Folder where you want to save the file
    save_folder = "./downloads"
    
    # File name to save locally
    file_name = "OpenSSH_log.tar.gz"

    # Call the function to download the CSV file
    text_file_download(source_url, save_folder, file_name)
