import argparse
import json
import logging
import os.path
import re
import subprocess
import sys

import pybitbucket.auth
from pybitbucket import bitbucket
# Those imports have side-effects, thus needed. pylint: disable=unused-import
from pybitbucket import comment as _comment
from pybitbucket import pullrequest as _pullrequest
from pybitbucket import repository as _repository
from pybitbucket import user as _user
# pylint: enable=unused-import
import uritemplate


# The built-in methods of the bitbucket SDK do not support queries and sort on pullrequest.
# Adding it here manually.
PR_BY_QUERY_ENDPOINT = {
    "_links": {
        "repositoryPullRequestsByQuery": {
            "href": ("https://api.bitbucket.org/2.0/repositories{/owner,repository_name}"
                     "/pullrequests{?q,sort}"),
        },
    },
}
POST_COMMENT_TEMPLATE = ("{+bitbucket_url}/2.0/repositories{/owner,repository_name}"
                         "/pullrequests{/pullrequest_id}/comments")
DIFF_SCOPE_RE = re.compile(r"^@@ -\d+,\d+ \+(\d+),(\d+) @@")


class PullRequest:
    def __init__(self, pr, user):
        self.id = pr.id
        self._pr = pr
        self._user = user

    def get_changed_lines(self, file_ext):
        """Retruns a mapping between changed files and a set of changed line numbers."""

        res = {}
        # Filtering out deleted files, and files not ending with file_ext.
        # TODO: Consider skipping files that are only moved (without changes).
        for diffstat in self._pr.diffstat():
            if diffstat["status"] != "removed" and diffstat["new"]["path"].endswith(file_ext):
                res[diffstat["new"]["path"]] = set()

        fname = None
        for line in self._pr.diff().decode().split("\n"):
            if line.startswith("+++ b/"):
                fname = line[6:]
            if line.startswith("@@ -") and fname in res:
                match = DIFF_SCOPE_RE.match(line)
                start = int(match.group(1))
                length = int(match.group(2))
                res[fname].update(range(start, start+length))
        return res

    def get_comments(self):
        return set((c.inline["path"], c.inline["to"], c.content["raw"])
                   for c in self._pr.comments() if not isinstance(c, dict) and
                   not c.deleted and c.user["uuid"] == self._user.uuid and
                   "inline" in c.attributes())

    def post_comment(self, path, line, content):
        owner, repository_name = self._pr.source_repository.full_name.split("/")
        api_uri = uritemplate.expand(POST_COMMENT_TEMPLATE, {
            "bitbucket_url": self._pr.client.get_bitbucket_url(),
            "owner": owner,
            "repository_name": repository_name,
            "pullrequest_id": self._pr.id,
        })
        data = {
            "content": {
                "raw": content,
            },
            "inline": {
                "path": path,
                "to": line,
            },
        }
        resp = self._pr.client.session.post(api_uri, json=data)
        if not resp.ok:
            logger.error("Failed posting comment on %s:%s: %s", path, line, resp.text)

    def approve(self):
        self._pr.approve()

    def unapprove(self):
        self._pr.unapprove()

    @staticmethod
    def get_pull_request(username, password, email, repository_name, owner, branch_name):
        """Returns the latest updated OPEN pull request for a particular branch."""
        client = bitbucket.Client(pybitbucket.auth.BasicAuthenticator(username, password, email))
        bb = bitbucket.Bitbucket(client)
        bb.add_remote_relationship_methods(PR_BY_QUERY_ENDPOINT)

        res = bb.repositoryPullRequestsByQuery(
            owner=owner,
            repository_name=repository_name,
            q='source.branch.name = "%s" AND state = "OPEN"' % branch_name, sort="-updated_on")

        pr = next(res)
        # This is weird, but if there's no data, we're getting a dict back.
        if isinstance(pr, dict):
            return None
        user = next(bb.userForMyself())
        return PullRequest(pr, user)


def run_pylint(linter, files_to_lint):
    pylint_proc = subprocess.run(
        [linter, "--output-format=json"] + list(files_to_lint),
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if pylint_proc.returncode >= 32:
        pylint_proc.check_returncode()

    pylint_output = json.loads(pylint_proc.stdout)
    # Sometimes we need to canonicalize path to match bitbucket's output.
    for lint_entry in pylint_output:
        if lint_entry["path"].startswith("/"):
            lint_entry["path"] = os.path.relpath(lint_entry["path"])

    return pylint_output


def lint_pr(pr, linter, approve):
    logging.info("Running pylint for PR %s.", pr.id)
    changed_lines = pr.get_changed_lines(".py")
    pylint_output = run_pylint(linter, changed_lines.keys())
    comments = pr.get_comments()

    approved = True
    for lint_entry in pylint_output:
        if lint_entry["line"] not in changed_lines[lint_entry["path"]]:
            logging.info("Skipping comment on %s:%d, not in the PR scope.",
                         lint_entry["path"], lint_entry["line"])
            continue

        approved = False
        content = "%(type)s (%(message-id)s %(symbol)s):\n\n> %(message)s" % lint_entry

        comment_key = (lint_entry["path"], lint_entry["line"], content)
        if comment_key in comments:
            logging.info("Skipping comment on %s:%d, already in the PR.",
                         lint_entry["path"], lint_entry["line"])
            continue

        logging.info("Posting comment on %s:%d.", lint_entry["path"], lint_entry["line"])
        pr.post_comment(*comment_key)

    if not approve:
        logging.info("Not %s PR, --approve=false.", "approving" if approved else "unapproving")
        return

    if approved:
        logging.info("Approving PR.")
        pr.approve()
        return

    logging.info("Unapproving PR.")
    pr.unapprove()


def main():
    parser = argparse.ArgumentParser(
        description="Runs pylint on changed py files of a Bitbucket PR, and leaves comments.")
    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("email")
    parser.add_argument("owner")
    parser.add_argument("repository")
    parser.add_argument("branch")
    parser.add_argument("--linter", default="pylint")
    parser.add_argument("--approve", type=bool, default=False)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    pr = PullRequest.get_pull_request(
        args.username, args.password, args.email, args.owner, args.repository, args.branch)
    if pr is None:
        logging.warning("No PR found for branch '%s'. Exiting", args.branch)
        return 1
    lint_pr(pr, args.linter, args.approve)
    return 0


if __name__ == "__main__":
    sys.exit(main())
