from pathlib import Path
import subprocess


class GitRepo:
    def __init__(self, repo_path: Path):
        # check that the git repo is initialized
        self.initialized = (repo_path / ".git").is_dir()
        self.repo_path = repo_path

    def _run(self, *args):
        result = subprocess.run(
            ["git", *args],
            cwd=str(self.repo_path),
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            if "nothing to commit" not in result.stdout:
                raise RuntimeError("Error when calling Git", result.stdout + ", " + result.stderr)

    def init(self):
        if not self.initialized:
            self._run("init")
            self._run("commit", "--message", "init", "--allow-empty")
        self.initialized = True

    def commit_all(self, msg=None, allow_empty=True):
        # make sure that the repo is initialized
        self.init()

        self._run("add", "--all")
        commit_args = []
        if allow_empty:
            commit_args.append("--allow-empty")
        self._run("commit", *commit_args, "--message", msg)
