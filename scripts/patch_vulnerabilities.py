#!/usr/bin/env python3
import importlib.metadata
import importlib.util
import shutil
import sys
from pathlib import Path


def patch_setuptools() -> None:
    print("Starting setuptools patch process...")

    # 1. Locate setuptools
    spec = importlib.util.find_spec("setuptools")
    if not spec or not spec.origin:
        print("Error: setuptools not found.")
        sys.exit(1)

    setuptools_path = Path(spec.origin).parent
    vendor_path = setuptools_path / "_vendor"
    jaraco_path = vendor_path / "jaraco"

    if not jaraco_path.exists():
        print(f"Error: jaraco vendor directory not found at {jaraco_path}")
        sys.exit(1)

    print(f"Setuptools vendor path: {vendor_path}")

    # 2. Locate the new jaraco.context package
    try:
        new_jaraco_spec = importlib.util.find_spec("jaraco.context")
        if not new_jaraco_spec or not new_jaraco_spec.origin:
            print("Error: jaraco.context package not found. Is it installed?")
            sys.exit(1)
        new_context_origin = Path(new_jaraco_spec.origin)
    except Exception as e:
        print(f"Error finding jaraco.context: {e}")
        sys.exit(1)

    print(f"New jaraco.context source: {new_context_origin}")

    # 3. Overwrite context.py
    # If source is a package (dir/__init__.py), copy __init__.py
    # If source is a module (file.py), copy file.py
    dest_context_file = jaraco_path / "context.py"
    if not dest_context_file.exists():
        print(f"Warning: {dest_context_file} does not exist. Creating it.")

    print(f"Overwriting {dest_context_file}...")
    shutil.copy2(new_context_origin, dest_context_file)

    # 4. Update metadata
    try:
        dist = importlib.metadata.distribution("jaraco.context")
        print(f"Installed jaraco.context version: {dist.version}")

        # Robustly locate site-packages containing the dist-info
        # Traverse up from the file origin until we find the dist-info directory
        current_path = new_context_origin.parent
        src_dist_info = None

        # NOTE: The distribution name can be 'jaraco.context' or 'jaraco_context' depending on the wheel builder.
        # We need to check both patterns.
        dist_patterns = [
            f"jaraco.context-{dist.version}.dist-info",
            f"jaraco_context-{dist.version}.dist-info"
        ]

        # Limit traversal to avoid infinite loop (e.g. 5 levels up)
        for _ in range(5):
            for pattern in dist_patterns:
                candidates = list(current_path.glob(pattern))
                if candidates:
                    src_dist_info = candidates[0]
                    break
            if src_dist_info:
                break
            if current_path == current_path.parent: # Root reached
                break
            current_path = current_path.parent

        if not src_dist_info:
             print(f"Error: Could not locate dist-info (checked {dist_patterns}) starting from {new_context_origin}")
             sys.exit(1)

        print(f"Source dist-info: {src_dist_info}")

        # Remove old dist-info from setuptools vendor
        # Note: setuptools vendors it as 'jaraco.context' usually, but we should clear both just in case
        old_dist_infos = list(vendor_path.glob("jaraco.context-*.dist-info")) + list(
            vendor_path.glob("jaraco_context-*.dist-info")
        )
        for old in old_dist_infos:
            print(f"Removing old metadata: {old}")
            shutil.rmtree(old)

        # Copy new dist-info
        # We must rename it to match what setuptools expects if it was underscore
        # But actually, keeping it as is usually works, or we can standardize on 'jaraco.context'
        # Setuptools 80.9.0 seems to use 'jaraco.context' based on the user's scan report.
        # "jaraco.context-5.3.0.dist-info"

        dest_name = f"jaraco.context-{dist.version}.dist-info"
        dest_dist_info = vendor_path / dest_name

        print(f"Copying metadata to {dest_dist_info}...")
        if dest_dist_info.exists():
             shutil.rmtree(dest_dist_info)
        shutil.copytree(src_dist_info, dest_dist_info)

    except Exception as e:
        print(f"Error updating metadata: {e}")
        sys.exit(1)

    print("Successfully patched setuptools.")


if __name__ == "__main__":
    patch_setuptools()
