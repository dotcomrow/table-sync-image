#!/bin/bash

# Script to find and pull the latest timestamped image from GitHub Container Registry
# Usage: ./get-latest-image.sh [registry/owner/repo] [output-format]

set -e

# Configuration
REGISTRY_URL="ghcr.io"
DEFAULT_REPO="dotcomrow/table-sync-image"
REPO="${1:-$DEFAULT_REPO}"
OUTPUT_FORMAT="${2:-image}"  # Options: image, tag, yaml, json

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔍 Finding latest timestamped image for ${REGISTRY_URL}/${REPO}${NC}"

# Function to get all tags from GitHub Container Registry
get_all_tags() {
    local repo=$1
    local page=1
    local all_tags=""
    
    while true; do
        # Use GitHub API to get package versions (tags)
        local response=$(curl -s \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/orgs/$(echo $repo | cut -d'/' -f1)/packages/container/$(echo $repo | cut -d'/' -f2)/versions?page=$page&per_page=100" 2>/dev/null || echo "[]")
        
        if [ "$response" = "[]" ] || [ "$(echo "$response" | jq length 2>/dev/null || echo 0)" -eq 0 ]; then
            break
        fi
        
        # Extract tags from the response
        local page_tags=$(echo "$response" | jq -r '.[].metadata.container.tags[]? // empty' 2>/dev/null | grep -E '^ts-[0-9]{8}-[0-9]{6}$' || true)
        
        if [ -z "$page_tags" ]; then
            break
        fi
        
        all_tags="$all_tags$page_tags"$'\n'
        ((page++))
        
        # Safety break after 10 pages
        if [ $page -gt 10 ]; then
            break
        fi
    done
    
    echo "$all_tags" | grep -v '^$' | sort -u
}

# Function to find latest timestamp tag
find_latest_tag() {
    local tags="$1"
    local latest_tag=""
    local latest_timestamp=""
    
    while IFS= read -r tag; do
        if [[ $tag =~ ^ts-([0-9]{8})-([0-9]{6})$ ]]; then
            local date_part="${BASH_REMATCH[1]}"
            local time_part="${BASH_REMATCH[2]}"
            local timestamp="${date_part}${time_part}"
            
            if [[ "$timestamp" > "$latest_timestamp" ]]; then
                latest_timestamp="$timestamp"
                latest_tag="$tag"
            fi
        fi
    done <<< "$tags"
    
    echo "$latest_tag"
}

# Try to get tags using GitHub API
echo -e "${YELLOW}📡 Querying GitHub Container Registry API...${NC}"
tags=$(get_all_tags "$REPO")

if [ -z "$tags" ]; then
    echo -e "${YELLOW}⚠️  GitHub API method failed, trying Docker Registry API...${NC}"
    
    # Fallback: Try Docker Registry API (may require authentication)
    tags=$(curl -s "https://${REGISTRY_URL}/v2/${REPO}/tags/list" 2>/dev/null | jq -r '.tags[]? // empty' 2>/dev/null | grep -E '^ts-[0-9]{8}-[0-9]{6}$' || true)
fi

if [ -z "$tags" ]; then
    echo -e "${RED}❌ Failed to retrieve tags from registry${NC}"
    echo -e "${YELLOW}💡 This might be due to:${NC}"
    echo "   - Private repository requiring authentication"
    echo "   - Network connectivity issues"
    echo "   - Registry API limitations"
    echo ""
    echo -e "${BLUE}🔧 Manual alternatives:${NC}"
    echo "   1. Use GitHub web interface: https://github.com/dotcomrow/table-sync-image/pkgs/container/table-sync-image"
    echo "   2. Use GitHub CLI: gh api orgs/dotcomrow/packages/container/table-sync-image/versions"
    echo "   3. Set GITHUB_TOKEN environment variable for API access"
    exit 1
fi

echo -e "${GREEN}✅ Found $(echo "$tags" | wc -l) timestamped tags${NC}"

# Find the latest tag
latest_tag=$(find_latest_tag "$tags")

if [ -z "$latest_tag" ]; then
    echo -e "${RED}❌ No valid timestamped tags found${NC}"
    exit 1
fi

# Parse the timestamp for display
if [[ $latest_tag =~ ^ts-([0-9]{4})([0-9]{2})([0-9]{2})-([0-9]{2})([0-9]{2})([0-9]{2})$ ]]; then
    year="${BASH_REMATCH[1]}"
    month="${BASH_REMATCH[2]}"
    day="${BASH_REMATCH[3]}"
    hour="${BASH_REMATCH[4]}"
    minute="${BASH_REMATCH[5]}"
    second="${BASH_REMATCH[6]}"
    
    readable_time="${year}-${month}-${day} ${hour}:${minute}:${second} UTC"
else
    readable_time="Unknown"
fi

full_image="${REGISTRY_URL}/${REPO}:${latest_tag}"

echo -e "${GREEN}🎯 Latest image found:${NC}"
echo -e "   Tag: ${BLUE}${latest_tag}${NC}"
echo -e "   Time: ${BLUE}${readable_time}${NC}"
echo -e "   Image: ${BLUE}${full_image}${NC}"

# Output based on requested format
case "$OUTPUT_FORMAT" in
    "tag")
        echo "$latest_tag"
        ;;
    "image")
        echo "$full_image"
        ;;
    "yaml")
        echo "image: $full_image"
        ;;
    "json")
        cat << EOF
{
  "tag": "$latest_tag",
  "image": "$full_image",
  "timestamp": "$readable_time",
  "registry": "$REGISTRY_URL",
  "repository": "$REPO"
}
EOF
        ;;
    *)
        echo "$full_image"
        ;;
esac

# Optional: Pull the image
read -p "$(echo -e ${YELLOW}🚀 Pull this image now? [y/N]: ${NC})" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}📥 Pulling ${full_image}...${NC}"
    docker pull "$full_image"
    echo -e "${GREEN}✅ Image pulled successfully${NC}"
fi