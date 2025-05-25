import os
from git import Repo, GitCommandError

# Configuration
REPO_URL = 'https://github.com/your-username/your-repo.git'  # Replace with your repository URL
LOCAL_DIR = '/path/to/local/repo'  # Replace with your desired local directory
BRANCH_NAME = 'feature-branch'  # Replace with your desired branch name
COMMIT_MESSAGE = 'Add new feature'  # Replace with your commit message

def clone_repository(repo_url, local_dir):
    if os.path.exists(local_dir):
        print(f"Directory {local_dir} already exists. Skipping clone.")
        return Repo(local_dir)
    try:
        print(f"Cloning repository from {repo_url} to {local_dir}...")
        repo = Repo.clone_from(repo_url, local_dir)
        print("Repository cloned successfully.")
        return repo
    except GitCommandError as e:
        print(f"Error cloning repository: {e}")
        return None

def create_and_checkout_branch(repo, branch_name):
    try:
        print(f"Creating and checking out branch '{branch_name}'...")
        new_branch = repo.create_head(branch_name)
        new_branch.checkout()
        print(f"Switched to branch '{branch_name}'.")
    except GitCommandError as e:
        print(f"Error creating or checking out branch: {e}")

def add_and_commit_changes(repo, commit_message):
    try:
        print("Adding changes...")
        repo.git.add(A=True)
        print("Committing changes...")
        repo.index.commit(commit_message)
        print("Changes committed.")
    except GitCommandError as e:
        print(f"Error adding or committing changes: {e}")

def push_changes(repo, branch_name):
    try:
        print(f"Pushing changes to remote branch '{branch_name}'...")
        origin = repo.remote(name='origin')
        origin.push(refspec=f'{branch_name}:{branch_name}')
        print("Changes pushed successfully.")
    except GitCommandError as e:
        print(f"Error pushing changes: {e}")

def main():
    repo = clone_repository(REPO_URL, LOCAL_DIR)
    if repo is None:
        return
    create_and_checkout_branch(repo, BRANCH_NAME)
    # Make your changes here, e.g., create or modify files in LOCAL_DIR
    add_and_commit_changes(repo, COMMIT_MESSAGE)
    push_changes(repo, BRANCH_NAME)

if __name__ == '__main__':
    main()

