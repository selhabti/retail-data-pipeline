#!/bin/bash
# scripts/changelog.sh

echo "# Changelog" > CHANGELOG.md
echo "" >> CHANGELOG.md

git tag -l --sort=-version:refname | while read tag; do
    echo "## $tag" >> CHANGELOG.md
    git log --oneline --pretty=format:"- %s" $tag...HEAD | head -20 >> CHANGELOG.md
    echo "" >> CHANGELOG.md
done

echo "✅ CHANGELOG.md generated!"
