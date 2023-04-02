import os
import time
import tempfile
import hashlib
from typing import Union
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import aiofiles


async def get_git_files_block(session: aiohttp.ClientSession,
                              url: str
                              ) -> Union[list, str]:
    '''
    Function returns the HTML block with directories and files links
    '''

    response = await session.get(url=url)
    soup = BeautifulSoup(await response.text(), 'lxml')
    files_block = soup.find_all("td", class_="four")
    if not files_block:
        raise ValueError("Incorrect URL. Example of correct URL: https://gitea.radium.group/radium/my_project")

    return files_block


async def collect_dirs(session: aiohttp.ClientSession, repo_url: str,
                       directory_url: str = None, directories: list = None
                       ) -> list:

    '''
    Function collects directories links from repository in a list (including the root of repository)
    '''

    dirs = []
    directories = [repo_url] if directories is None else directories
    directory_url = repo_url if directory_url is None else directory_url

    files_block = await get_git_files_block(session, directory_url)

    for file in files_block:

        link = str(file).split("href=\"")[1].split('\" title')[0].split("src")[1]

        if "octicon-file-directory-fill" in str(file):
            dir_link = f'{repo_url}/src{link}'
            if dir_link not in dirs:
                dirs.append(dir_link)

    if dirs:
        for dir_link in dirs:
            directories.append(dir_link)

    for dir_link in dirs:
        await collect_dirs(session=session, repo_url=repo_url, directory_url=dir_link, directories=directories)

    return directories


async def get_urls(session: aiohttp.ClientSession, directories: list) -> list:
    '''
    Function collects files urls from all directories in a list
    '''

    repo_url = directories[0]
    links_list = []
    for dir_link in directories:
        files_block = await get_git_files_block(session, dir_link)
        for file in files_block:

            link = str(file).split("href=\"")[1].split('\" title')[0].split("src/")[-1]

            if "octicon-file-directory-fill" not in str(file):
                file_link = f'{repo_url}/raw/{link}'
                links_list.append(file_link)

    return links_list


async def create_folders(folder_name: str, directories: list) -> list:
    '''
    Function creates folders (the same as repository has) in temporary directory
    and returns absolute paths of these folders
    '''

    git_directory_name = directories[0].split('/')[-1]
    repository_root = f'{folder_name}\\{git_directory_name}'
    directory_paths = [repository_root]
    if not os.path.exists(repository_root):
        os.makedirs(repository_root)
    if len(directories) > 1:
        for dir_link in directories[1:]:
            dir_path = f'{repository_root}\\{dir_link.split("master/")[-1]}'
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            directory_paths.append(dir_path)

    return directory_paths


async def download_file_tasks(session: aiohttp.ClientSession, folder_name: str, directory_paths: list,
                              link: str
                              ) -> None:
    '''
    Function downloads files from repository in the temporary directory
    '''

    repo_root_name = directory_paths[0].split(f'{folder_name}\\')[-1].replace('\\', '/')
    git_user_link = link.split(repo_root_name)[0]
    for dir_name in directory_paths:
        relative_way_to_dir = dir_name.split(f'{folder_name}\\')[-1].replace('\\', '/')
        relative_way_to_file = "".join(link.split("raw/branch/master/")).split(git_user_link)[-1]
        file_name = relative_way_to_file.split('/')[-1]
        if relative_way_to_dir == relative_way_to_file.split(f'/{file_name}')[0]:
            response = await session.get(url=link)
            async with aiofiles.open(os.path.join(f'{folder_name}\\{relative_way_to_dir}', file_name), 'wb') as f:
                async for data in response.content.iter_any():
                    await f.write(data)
                    paths.append(f'{folder_name}\\{relative_way_to_dir}\\{file_name}'.replace('/', '\\'))


def hashing(paths: list) -> None:
    '''
    Hashing (sha256) downloaded files.
    '''

    if paths:
        print("SHA256: ")
        for path in paths:
            with open(path, 'rb') as f:
                content = f.read()
                sha256 = hashlib.sha256()
                sha256.update(content)
            print(f'{f.name}: {sha256.hexdigest()}', end='\n\n')
    else:
        print('There are no files for hashing')


async def main():
    from tqdm.asyncio import tqdm_asyncio

    temp_dir = tempfile.mkdtemp()

    first_url = "https://gitea.radium.group/radium/project-configuration"

    async with aiohttp.ClientSession() as session:
        directories = await collect_dirs(session, repo_url=first_url)
        files = await get_urls(session=session, directories=directories)
        directory_paths = await create_folders(temp_dir, directories)

        await tqdm_asyncio.gather(*[download_file_tasks(session, temp_dir, directory_paths, file)
                                    for file in files], total=len(files))


if __name__ == "__main__":
    paths = []
    t1 = time.perf_counter()
    print("process started...")
    asyncio.get_event_loop().run_until_complete(main())
    t2 = time.perf_counter()
    print(f'Completed in {t2-t1} seconds.', end='\n\n')
    hashing(paths)
