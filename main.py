import concurrent.futures
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

import backoff
import colored
import git
import pandas as pd
import progressbar
import requests
import yaml
from colored import stylize

GH_API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com")
GH_PAT = os.environ["GITHUB_TOKEN"]

# Supported IAC: Terraform, Cloudformation, k8s`
IAC_EXTENSIONS = ["tf", "json", "yml", "yaml"]

# Constants for coloured output
STYLE_INFO = colored.fg("blue") + colored.attr("bold")
STYLE_ERR = colored.fg("red") + colored.attr("bold")
STYLE_WARN = colored.fg("yellow") + colored.attr("bold")
STYLE_SUCCESS = colored.fg("green") + colored.attr("bold")

# GH auth token headers
headers = {"Authorization": f"token {GH_PAT}", "Accept": "application/vnd.github+json"}

# Logger set up, INFO as default level
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.DEBUG)
# formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# handler.setFormatter(formatter)
# logger.addHandler(handler)


@backoff.on_exception(backoff.expo, Exception, max_tries=30)
def get_repos(org_name):
    """
    Will get all repositories within a GitHub organisation
    :param org_name: the name of the organisation
    :return: a list of all repositories within that organisation
    """
    logger.debug(f"Getting GitHub repos from API")
    repos = []
    url = f"{GH_API_BASE}/orgs/{org_name}/repos?per_page=100"
    page = 1
    while url:
        logger.debug(f"Getting page {page} of repos...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        repos.extend([x for x in response.json() if not x["fork"]])  # dont get forks
        url = response.links.get("next", {}).get("url")
        page += 1
    return repos


def get_github_orgs():
    """
    Retrieve a list of organizations the authenticated user has access to using the GitHub API.

    :return: A list of organization names the authenticated user has access to.
    """
    url = f"{GH_API_BASE}/user/orgs"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        orgs = response.json()
        org_names = [org["login"] for org in orgs]
        return org_names
    else:
        logger.error("Error getting list of orgs! ")
        return []


def get_files_in_commit_tree(tree):
    """
    Will recursively get all the files within a commit tree
    :param tree_url: the commit tree URL
    :param path: the current path (starts at nothing, ie. the root)
    :return: the list of files associated with the passed commit tree url
    """
    files = []
    for blob in tree.blobs:
        files.append(blob.path)

    for tree in tree.trees:
        files.extend(get_files_in_commit_tree(tree))

    return files


def contents_are_iac(file_path):
    if file_path.endswith("tf"):
        return True

    with open(file_path, "r", encoding="utf-8", errors="replace") as file:
        try:
            contents = file.read()

            if any(file_path.endswith(ext) for ext in ("yml", "yaml", "json")):
                if file_path.endswith("json"):
                    loaded = json.loads(contents)
                else:
                    loaded = yaml.safe_load(contents)

                if type(loaded) is dict:
                    # Check for cloudformation
                    if loaded.get("Resources"):
                        return True
                    # Check for kubernetes manifests
                    elif loaded.get("apiVersion"):
                        return True
        except:
            return False
    return False


def scan_repo_for_iac_files(search_dir):
    """
    Will recursively scan a repository for IAC files
    :param repo: the repository that we're scanning
    :param path: the current path (starts at the root of the git project)
    :return: the list of files with an IAC extension
    """
    found_files = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for root, _, files in os.walk(search_dir):
            file_paths = [os.path.join(root, f) for f in files]
            results = list(executor.map(contents_are_iac, file_paths))

            for file_path, result in zip(file_paths, results):
                if result:
                    found_files.append(os.path.relpath(file_path, search_dir))

    return found_files


def get_unique_contributors(commits):
    """
    Will get an accurate count of unique contributors based on the commit history
    :param commits: the commit history
    :param days: the last n days that we'll search the history for
    :return: the number of unique contributors
    """
    contributors = []
    for commit in commits:
        contributors.append(commit.author.email)
    return contributors


def write_excel_data(file_name, raw_data, total_contributors):
    """
    Will write the data that the tool has gathered to an excel file
    :param raw_data: the raw data
    :param total_contributors: the de-duped list of contributors
    """
    repo_rows = []

    # Generate the excel data based on our raw captured data
    for item in raw_data:
        repo = item[0]
        commit_history = item[1]
        commit_history_shas = [x.hexsha for x in commit_history]
        iac_files = item[2]
        contributors = item[3]
        repo_rows.append(
            [
                repo,
                ",".join(iac_files),
                len(iac_files),
                ",".join(commit_history_shas),
                len(commit_history),
                ",".join(contributors),
                len(contributors),
            ]
        )

    # Add a bit in to show total contributors
    repo_rows.append([])  # Blank line
    repo_rows.append(["Total unique contributors: ", f"{len(total_contributors)}"])
    df = pd.DataFrame(
        repo_rows,
        columns=[
            "Repository",
            "IAC files",
            "# IAC files",
            "Commits",
            "# Commits",
            "Contributors",
            "# Contributors",
        ],
    )
    df.to_excel(f"{file_name}.xlsx", index=False, engine="openpyxl")


def unique_contributors_for_repo(repo):
    try:
        with tempfile.TemporaryDirectory() as temporary_dir:
            logger.debug(
                f"[{repo['name']}] Cloning repo to temporary directory {temporary_dir}"
            )
            # First we need to try and clone down the repository to a temporary folder
            try:
                git.Repo.clone_from(repo["clone_url"], temporary_dir)
            except git.GitCommandError as e:
                logger.error(f"[{repo['name']}] Error cloning repo")
                return None, None, None

            # Now we've cloned the repo down, we can interact with it
            local_repo = git.Repo(temporary_dir)

            # Now we'll search for potential IAC files, if none are found we skip this repo
            iac_files_found = scan_repo_for_iac_files(temporary_dir)
            if len(iac_files_found) == 0:
                logger.debug(f"[{repo['name']}] No IAC files found")
                return None, None, None
            logger.debug(f"[{repo['name']}] Found {len(iac_files_found)}  iac files")
            logger.debug("IAC files found: " + ",".join(iac_files_found))

            # Firstly, let's filter down our commits so we've only got the last 90 days
            commit_history = list(local_repo.iter_commits())
            time_filtered_commit_history = []
            for commit in commit_history:
                commit_datetime = datetime.fromtimestamp(commit.committed_date)
                cutoff_date = datetime.utcnow() - timedelta(days=90)
                if commit_datetime >= cutoff_date:
                    time_filtered_commit_history.append(commit)
            logger.debug(
                f"[{repo['name']}] Found {len(time_filtered_commit_history)} commits in last 90 days"
            )

            # Now we'll filter down based only on commits for IAC files
            iac_filtered_commits = []
            for commit in time_filtered_commit_history:
                commit_files = list(set(get_files_in_commit_tree(commit.tree)))
                matched_files = set(commit_files) & set(iac_files_found)
                if len(matched_files) > 0:
                    iac_filtered_commits.append(commit)
            if len(iac_filtered_commits) == 0:
                logger.debug(f"[{repo['name']}] No IAC files found after deep scan")
                return None, None, None
            logger.debug(
                f"[{repo['name']}] Found {len(iac_filtered_commits)} commits with valid IAC files in tree"
            )

            # Count the number of unique contributors based on our valid commits
            repo_unique_contributors = list(
                set(get_unique_contributors(iac_filtered_commits))
            )
            return repo_unique_contributors, iac_filtered_commits, iac_files_found
    except:
        return None, None, None


def ignore(loader, tag, node):
    classname = node.__class__.__name__
    if classname == "SequenceNode":
        resolved = loader.construct_sequence(node)
    elif classname == "MappingNode":
        resolved = loader.construct_mapping(node)
    else:
        resolved = loader.construct_scalar(node)
    return resolved


yaml.add_multi_constructor("!", ignore, Loader=yaml.SafeLoader)
yaml.add_multi_constructor("", ignore, Loader=yaml.SafeLoader)


def process_org(gh_org_name):
    repos = get_repos(gh_org_name)
    org_unique_contributors = []
    reporting_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_cache = {
            executor.submit(unique_contributors_for_repo, repo): repo for repo in repos
        }

        # Progress bar so we can show how far along we are
        widgets = [
            "Processing ",
            progressbar.Counter(),
            " of ",
            progressbar.FormatLabel("%(max_value)d - "),
            progressbar.Bar(),
            progressbar.AdaptiveETA(),
        ]
        bar = progressbar.ProgressBar(
            widgets=widgets, max_value=len(future_cache)
        ).start()

        for future in concurrent.futures.as_completed(future_cache):
            repo = future_cache[future]
            (
                repo_unique_contributors,
                iac_filtered_commits,
                iac_files_found,
            ) = future.result()
            if repo_unique_contributors:
                org_unique_contributors.extend(repo_unique_contributors)

                # And append some reporting data
                reporting_data.append(
                    [
                        repo["name"],
                        iac_filtered_commits,
                        iac_files_found,
                        repo_unique_contributors,
                    ]
                )
                bar.update(bar.value + 1)
        bar.finish()

    # De-dupe the contributors from across multiple repos
    org_unique_contributors = list(set(org_unique_contributors))
    print(
        stylize(
            f"After de-duplication {len(org_unique_contributors)} unique contributors for IAC files were found (last 90 days)",
            STYLE_SUCCESS,
        )
    )

    if len(org_unique_contributors) > 0:
        # Generate a report for review
        write_excel_data(
            f"{gh_org_name}_output", reporting_data, org_unique_contributors
        )


def main():
    gh_orgs = get_github_orgs()
    for org in gh_orgs:
        print(stylize(f"Processing GH org {org}", STYLE_INFO))
        try:
            process_org(org)
            print(stylize(f"Done with processing {org}", STYLE_SUCCESS))
        except:
            print(stylize(f"Error processing {org} - skipping", STYLE_ERR))
            logger.error("Error processing org, will skip this one...")


if __name__ == "__main__":
    main()
