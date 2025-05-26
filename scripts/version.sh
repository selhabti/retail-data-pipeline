#!/bin/bash
# scripts/version.sh

VERSION=$1
if [ -z "$VERSION" ]; then
    echo "Usage: ./scripts/version.sh <version>"
    echo "Example: ./scripts/version.sh v1.2.0"
    exit 1
fi

# Créer le tag
git tag -a $VERSION -m "Release $VERSION"

# Pousser le tag (si un remote est configuré)
if git remote -v | grep -q origin; then
    git push origin $VERSION
    echo "✅ Version $VERSION created and pushed!"
else
    echo "✅ Version $VERSION created locally!"
    echo "⚠️ No remote repository configured. Tag was not pushed."
fi
