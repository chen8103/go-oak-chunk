#!/usr/bin/env bash
# Write a Chinese release title and body for an existing local tag.
# Does not create a tag or a GitHub Release.
#
# Title: annotated tag subject, otherwise the tag name.
# Body: tag annotation (when present) plus commits since the previous v3.* tag.
# The long-form notes on the historical v3.5.0 / v3.5.1 Releases were written
# by hand; this is the automated stand-in, not a copy of those pages.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <tag> <title-file> <notes-file>" >&2
  exit 2
fi

TAG=$1
TITLE_FILE=$2
NOTES_FILE=$3

if [[ ! "$TAG" =~ ^v3[.][A-Za-z0-9._+-]+$ ]]; then
  echo "invalid tag: ${TAG}" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

if ! git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "tag not found: ${TAG}" >&2
  exit 1
fi

subject=""
body=""
if [[ "$(git cat-file -t "refs/tags/${TAG}")" == "tag" ]]; then
  subject=$(git tag -l --format='%(contents:subject)' "$TAG")
  body=$(git tag -l --format='%(contents:body)' "$TAG")
fi

subject=${subject//$'\n'/}
subject=${subject#"${subject%%[![:space:]]*}"}
subject=${subject%"${subject##*[![:space:]]}"}
if [[ -z "$subject" ]]; then
  subject=$TAG
fi

SHA=$(git rev-parse "${TAG}^{}")
if prev=$(git describe --tags --abbrev=0 --match 'v3.*' "${TAG}^" 2>/dev/null); then
  range="${prev}..${TAG}"
  summary_head="自 \`${prev}\` 至 \`${TAG}\` 的提交："
else
  range=$TAG
  summary_head="自仓库起始至 \`${TAG}\` 的提交："
fi
commits=$(git log --pretty=format:'- %s (%h)' "$range")
if [[ -z "$commits" ]]; then
  commits="- （没有新提交）"
fi

export REL_SUBJECT="$subject"
export REL_BODY="$body"
export REL_SHA="$SHA"
export REL_SUMMARY_HEAD="$summary_head"
export REL_COMMITS="$commits"
export REL_TITLE_FILE="$TITLE_FILE"
export REL_NOTES_FILE="$NOTES_FILE"

python3 - <<'PY'
import os
import pathlib

body = os.environ["REL_BODY"].strip()
if not body:
    body = "该 tag 没有附注正文。"
notes = "\n".join(
    [
        "## 发布说明",
        "",
        body,
        "",
        "## 变更摘要",
        "",
        os.environ["REL_SUMMARY_HEAD"],
        "",
        os.environ["REL_COMMITS"],
        "",
        "发布提交：`" + os.environ["REL_SHA"] + "`",
        "",
    ]
)
pathlib.Path(os.environ["REL_TITLE_FILE"]).write_text(os.environ["REL_SUBJECT"] + "\n")
pathlib.Path(os.environ["REL_NOTES_FILE"]).write_text(notes)
PY
