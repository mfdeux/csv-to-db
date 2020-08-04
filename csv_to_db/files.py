import csv
import io
import os
import shutil
import tempfile
import typing
import zipfile
from glob import glob
from pathlib import Path

import git
import requests
from git import RemoteProgress
from tqdm import tqdm


def is_file(path: str, encoding: str = 'ISO-8859-1', newline=''):
    if Path(path).exists():
        return open(path, 'r', encoding=encoding)
    else:
        raise Exception('file does not exist')


def is_csv_file(fh: typing.Any):
    try:
        csv_reader = csv.reader(fh)
    except Exception as error:
        raise

    rows = list(csv_reader)
    # for row in csv_reader:
    #     print(row)
    print(rows)
    fh.close()


def csv_files(dirpath: str):
    files = []
    for ext in ('*.csv',):
        files.extend(glob(os.path.join(dirpath, ext)))
    return files


# check csv file
# check dir for csv files

def download_file_to_memory(url: str):
    try:
        r = requests.get(url, timeout=3)
        r.raise_for_status()
    except Exception as error:
        raise
    return io.StringIO(r.text)
    # with requests.get(url, stream=True) as r:
    #     total_size_in_bytes = int(r.headers.get('content-length', 0))
    #     block_size = 1024
    #     progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    #
    #     fh = io.BytesIO()
    #
    #     for data in r.iter_content(block_size):
    #         progress_bar.update(len(data))
    #         fh.write(data)
    #     progress_bar.close()
    #
    # print(fh.read())
    #
    # return fh


def download_file_to_temp_file(url: str):
    local_filename = url.split('/')[-1]

    with requests.get(url, stream=True) as r:
        total_size_in_bytes = int(r.headers.get('content-length', 0))
        block_size = 1024
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)

        # print(r.status_code)
        # print(r.text)

        with tempfile.NamedTemporaryFile(delete=False) as fh:
            for data in r.iter_content(block_size):
                progress_bar.update(len(data))
                fh.write(data)
                progress_bar.close()

    return fh.name, local_filename


def download_git_repo(repo: str):
    """
    Download remote git repo
    """
    local_filename = repo.split('/')[-1]

    class CloneProgress(RemoteProgress):
        def update(self, op_code, cur_count, max_count=None, message=''):
            if message:
                print(message)

    td = tempfile.mkdtemp()
    repo_local_path = os.path.join(td, local_filename)

    git.Repo.clone_from(repo, repo_local_path,
                        branch='master', progress=CloneProgress(), depth=1)
    return repo_local_path


def find_files_in_dir(path: str, patterns: typing.List[str] = None, recursive: bool = False) -> typing.List[Path]:
    """
    Find specified files in directory

    """
    if not patterns:
        patterns = ['*.csv']

    base_dir = Path(path)
    files = []

    for pattern in patterns:
        if recursive:
            for found_file in base_dir.rglob(pattern):
                if found_file.is_file():
                    files.append(found_file)
        else:
            for found_file in base_dir.glob(pattern):
                if found_file.is_file():
                    files.append(found_file)

    return files


def extract_zip_file(save_path: str = None):
    """

    Extract ZIP file and use contents as dir
    TODO: To come

    """
    z = zipfile.ZipFile(zip_fh)

    if not save_path:
        temp_dirpath = tempfile.mkdtemp()
        full_dirpath = os.path.join(temp_dirpath, local_pathname)
    else:
        full_dirpath = os.path.join(save_path, local_pathname)

    Path(full_dirpath).mkdir(parents=True, exist_ok=True)
    z.extractall(full_dirpath)

    return full_dirpath


def remove_dir(path: str):
    shutil.rmtree(path)

# rar file

# json
# xlsx


# shutil.move(os.path.join(t, 'setup.py'), '.')
# # Remove temporary dir
# shutil.rmtree(t)