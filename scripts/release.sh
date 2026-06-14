#!/usr/bin/env bash
# macOS sed syntax (sed -i ''). On Linux omit the empty string: sed -i 's/.../.../'.
set -euo pipefail

NEW_VERSION="${1:?Usage: $0 <new-version>  e.g. $0 0.4.0}"
NEW_VERSION="${NEW_VERSION#v}"   # strip leading 'v' if provided

echo "Bumping to version $NEW_VERSION"

sed -i '' \
  "s/^version *= *\"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" \
  crates/isam/Cargo.toml

sed -i '' \
  "s/^version *= *\"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" \
  crates/highlandcows/Cargo.toml
sed -i '' \
  "s/highlandcows-isam = { path = \"\.\.\/isam\", version = \"[0-9]*\.[0-9]*\.[0-9]*\" }/highlandcows-isam = { path = \"..\/isam\", version = \"$NEW_VERSION\" }/" \
  crates/highlandcows/Cargo.toml
sed -i '' \
  "s/highlandcows-eventkit = { path = \"\.\.\/eventkit\", version = \"[0-9]*\.[0-9]*\.[0-9]*\" }/highlandcows-eventkit = { path = \"..\/eventkit\", version = \"$NEW_VERSION\" }/" \
  crates/highlandcows/Cargo.toml

sed -i '' \
  "s/^version *= *\"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$NEW_VERSION\"/" \
  crates/eventkit/Cargo.toml

sed -i '' \
  "s/^highlandcows = \"[0-9]*\.[0-9]*\.[0-9]*\"/highlandcows = \"$NEW_VERSION\"/" \
  README.md
sed -i '' \
  "s/^highlandcows-isam = \"[0-9]*\.[0-9]*\.[0-9]*\"/highlandcows-isam = \"$NEW_VERSION\"/" \
  README.md
sed -i '' \
  "s/^highlandcows-eventkit = \"[0-9]*\.[0-9]*\.[0-9]*\"/highlandcows-eventkit = \"$NEW_VERSION\"/" \
  README.md

sed -i '' \
  "s/^highlandcows-isam = \"[0-9]*\.[0-9]*\.[0-9]*\"/highlandcows-isam = \"$NEW_VERSION\"/" \
  crates/isam/README.md

sed -i '' \
  "s/^highlandcows-eventkit = \"[0-9]*\.[0-9]*\.[0-9]*\"/highlandcows-eventkit = \"$NEW_VERSION\"/" \
  crates/eventkit/README.md

echo "Running cargo check..."
cargo check --workspace

git add crates/isam/Cargo.toml crates/highlandcows/Cargo.toml crates/eventkit/Cargo.toml Cargo.lock README.md crates/isam/README.md crates/eventkit/README.md
git commit -m "chore: bump versions to $NEW_VERSION"
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"

echo ""
echo "Done. Now run:"
echo "  git push && git push origin v$NEW_VERSION"
echo "  gh release create v$NEW_VERSION --repo njacobs5074/highlandcows --title \"v$NEW_VERSION\" --notes \"\"  # add release notes"
echo "  cargo publish -p highlandcows-isam"
echo "  cargo publish -p highlandcows"
echo "  cargo publish -p highlandcows-eventkit"
