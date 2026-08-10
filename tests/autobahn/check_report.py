"""Turn an Autobahn|Testsuite report into a pass/fail exit code.

The suite always exits 0 -- it reports, it does not judge -- so without this
the CI job would be green no matter what the results said.

Three behaviours are accepted:

* ``OK`` -- the case passed.
* ``NON-STRICT`` -- the case passed, but not in the strictest possible way.
  There is exactly one class of these here and it is a known, documented
  choice: UTF-8 is validated on the reassembled message rather than fail-fast
  mid-fragment (cases 6.4.x). Fail-fast validation is a hardening item, and
  the alternative -- per-fragment validation -- would reject legitimate
  traffic where a multi-byte character straddles a fragment boundary.
* ``INFORMATIONAL`` -- the suite is measuring, not asserting (section 9).

``UNIMPLEMENTED`` counts as a failure rather than a skip: a case the server
never answered is not a case that passed.
"""

from __future__ import annotations

import json
import pathlib
import sys

ACCEPTED = {"OK", "NON-STRICT", "INFORMATIONAL"}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_report.py <reports/servers/index.json>", file=sys.stderr)
        return 2
    index_path = pathlib.Path(sys.argv[1])
    if not index_path.exists():
        print(f"no report at {index_path}: the suite did not run", file=sys.stderr)
        return 2

    index = json.loads(index_path.read_text())
    failures: list[tuple[str, str, str]] = []
    total = 0
    non_strict = 0
    for agent, cases in index.items():
        for case, result in cases.items():
            total += 1
            behavior = result.get("behavior", "UNIMPLEMENTED")
            close = result.get("behaviorClose", "OK")
            if behavior == "NON-STRICT":
                non_strict += 1
            if behavior not in ACCEPTED:
                failures.append((agent, case, behavior))
            elif close not in ACCEPTED:
                failures.append((agent, case, f"close:{close}"))

    print(f"{total} cases, {len(failures)} failing, {non_strict} non-strict")
    for agent, case, behavior in failures:
        print(f"  FAIL {agent} {case}: {behavior}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
