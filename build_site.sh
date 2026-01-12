#!/bin/bash
# Build script for Philosophical Transactions Archive
# This builds the Jekyll site and runs Pagefind for search indexing

set -e

SITE_DIR="docs"
BUILD_DIR="docs/_site"

echo "=========================================="
echo "Philosophical Transactions Archive Builder"
echo "=========================================="

# Step 1: Build Jekyll site
echo ""
echo "[1/3] Building Jekyll site..."
cd "$SITE_DIR"

# Check if bundler is available
if command -v bundle &> /dev/null; then
    bundle exec jekyll build
else
    echo "Warning: Bundler not found, trying jekyll directly..."
    jekyll build
fi

cd ..

# Step 2: Run Pagefind
echo ""
echo "[2/3] Running Pagefind indexer..."
echo "    This will index ~8,100 articles with full-text OCR..."

# Check if pagefind is installed
if ! command -v pagefind &> /dev/null; then
    echo "Pagefind not found. Installing via npx..."
    npx pagefind --site "$BUILD_DIR" --output-path "$SITE_DIR/pagefind"
else
    pagefind --site "$BUILD_DIR" --output-path "$SITE_DIR/pagefind"
fi

echo "    Index size: $(du -sh "$SITE_DIR/pagefind" | cut -f1)"

# Step 3: Report results
echo ""
echo "[3/3] Build complete!"
echo ""
echo "Pagefind index created in: $SITE_DIR/pagefind/"
echo ""
echo "To preview locally:"
echo "  cd $SITE_DIR && bundle exec jekyll serve"
echo ""
echo "To deploy to GitHub Pages:"
echo "  1. Commit the pagefind/ directory"
echo "  2. git push to your repository"
echo ""
echo "=========================================="
