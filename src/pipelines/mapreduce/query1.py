import os
import sys


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from queries import main


if __name__ == "__main__":
    sys.argv = [sys.argv[0], "--query", "query1", *sys.argv[1:]]
    main()