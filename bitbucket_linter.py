#!/usr/bin/env python

import json
import subprocess
import sys

from pybitbucket.auth import BasicAuthenticator
from pybitbucket.bitbucket import Client
from pybitbucket.comment import Comment
from pybitbucket.pullrequest import PullRequest, PullRequestState
from pybitbucket.ref import Branch
from pylint import epylint as lint


POST_COMMENT_URL = ("https://api.bitbucket.org/1.0/repositories/"
                    "%(owner)s/%(repository)s/pullrequests/"
                    "%(pull_request_id)s/comments")
DIFF_FILE_START_MAGIC = "+++ b/"

PYLINT_MESSAGE_TEMPLATE = '{{"abspath":"{abspath}","line":{line},"column":{column},"msg":"{msg}","msg_id":"{msg_id}","category":"{category}"}}'


class BitbucketCommenter:
    """Posts comments to pull requests in BitBucket repositories."""
    
    def __init__(self, username, password, email, owner_username,
                       repository_name, branch_name):
        self.username = username
        self.password = password
        self.email = email
        self.owner_username = owner_username
        self.repository_name = repository_name
        self.branch_name = branch_name

        self.client = Client(BasicAuthenticator(username, password, email))

        self._fetch_pull_request()

    def get_diff_files(self):
        diff = str(self.pull_request.diff())
        diff_lines = diff.split("\\n")
        file_lines = filter(lambda dl: dl.startswith(DIFF_FILE_START_MAGIC),
                            diff_lines)
        filenames = [dl[len(DIFF_FILE_START_MAGIC):] for dl in file_lines]
        return filenames

    def get_comments(self):
        return self.pull_request.comments()

    def post_comment(self, content, filename, line_number):
        # Post the new comment.
        pr_post_comment_url = POST_COMMENT_URL % {
            "owner": self.owner_username,
            "repository": self.repository_name,
            "pull_request_id": self.pull_request.id,
        }
        pr_post_comment_data = {
            "content": content,
            "anchor": self.pull_request.source_commit["hash"],
            "dest_rev": self.pull_request.destination_commit["hash"],
            "filename": filename,
            "line_to": line_number,
        }
        self.client.session.post(pr_post_comment_url, data=pr_post_comment_data)

    def _fetch_pull_request(self):
        # Fetch the correct pull request, and abort if none can be found.
        pull_requests = PullRequest.find_pullrequests_for_repository_by_state(
                self.repository_name, owner=self.owner_username, client=self.client)
        branch_pull_requests = list(filter(
                lambda pr: (pr.source["branch"]["name"] == self.branch_name and
                            pr.state.upper() == PullRequestState.OPEN.upper()),
                pull_requests))
        if len(branch_pull_requests) != 1:
            print ("Error: Found %s open pull requests for branch '%s'!" %
                   (len(branch_pull_requests), branch_name))
            sys.exit(1)
        self.pull_request = branch_pull_requests[0]


class PyLinter:
    """Runs PyLint on the diff files from commenter, and adds comments."""

    def __init__(self, commenter):
        """Receives the commenter to use for adding comments."""
        self.commenter = commenter
        
    def run(self):
        """Runs the linter and adds the necessary comments."""
        # Run PyLint on the .py diff files.
        py_diff_files = self._get_py_diff_files()
        if not py_diff_files:
            return
        pylint_stdout = str(subprocess.check_output(
            "pylint --output-format=text --msg-template='%s' \"%s\"; exit 0" % (
                PYLINT_MESSAGE_TEMPLATE, '" "'.join(py_diff_files)),
            shell=True,
            stderr=subprocess.STDOUT))

        # Read the output of the PyLint run.
        pylint_output_lines = pylint_stdout[2:-1].split("\\n")
        print ("\n".join(pylint_output_lines))

        # Create the comment dictionary to make sure we don't repeat comments.
        comments_map = self._generate_comments_map()

        # Process the output of PyLint line by line.
        for line in pylint_output_lines:
            # The format we use is a json format, so try to load it.
            try:
                line = line.strip()
                data = json.loads(line)
            except:
                continue

            # Generate the message content.
            content = "%(category)s (%(msg_id)s): %(msg)s" % {
                "category": data["category"].upper(),
                "msg_id": data["msg_id"],
                "msg": data["msg"],
            }

            # Check if the comment already exists.
            comment_key = (str(data["line"]).strip().lower(),
                           str(data["abspath"]).strip().lower(),
                           str(content).strip().lower())
            if comment_key in comments_map:
                continue

            # Post the comment.
            self.commenter.post_comment(content, data["abspath"], data["line"])

    def _get_py_diff_files(self):
        diff_files = self.commenter.get_diff_files()
        return filter(lambda df: df.strip().endswith(".py"), diff_files)

    def _generate_comments_map(self):
        return set((str(c.inline["to"]).strip().lower(),
                    str(c.inline["path"]).strip().lower(),
                    str(c.content["raw"]).strip().lower())
                   for c in self.commenter.get_comments()
                   if type(c) == Comment and "inline" in c.data)
        

def main():
    args = sys.argv
    if len(args) < 7:
        print ("Usage: bamboo_linter.py <bitbucket username>"
                                      " <bitbucket password>"
                                      " <bitbucket email>"
                                      " <bitbucket repository owner>"
                                      " <bitbucket repository>"
                                      " <bitbucket branch name>")
        return 1

    # Gather all relevant command line arguments.
    username = args[1] # "bamboo-ci_armis"
    password = args[2] # "1fnQWXH1RqIZ3fwzAjKCHixc"
    email = args[3] #"bamboo-ci@armis.com"
    owner_username = args[4] #"armis-security"
    repository_name = args[5] #"armis"
    branch_name = args[6] #"nadir.test_pylint"

    # Run the linter.
    commenter = BitbucketCommenter(username, password, email, owner_username,
                                   repository_name, branch_name)
    linter = PyLinter(commenter)
    linter.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())

