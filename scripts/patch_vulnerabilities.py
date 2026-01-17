#!/usr/bin/env python3
import importlib.metadata
import importlib.util
import shutil
import sys
from pathlib import Path


def patch_setuptools():
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
        new_context_file = Path(new_jaraco_spec.origin)
    except Exception as e:
        print(f"Error finding jaraco.context: {e}")
        sys.exit(1)

    print(f"New jaraco.context source: {new_context_file}")

    # 3. Overwrite context.py
    dest_context_file = jaraco_path / "context.py"
    if not dest_context_file.exists():
        print(f"Warning: {dest_context_file} does not exist. Creating it.")

    print(f"Overwriting {dest_context_file}...")
    shutil.copy2(new_context_file, dest_context_file)

    # 4. Update metadata
    try:
        dist = importlib.metadata.distribution("jaraco.context")
        print(f"Installed jaraco.context version: {dist.version}")

        # Locate the source dist-info directory
        # new_context_file is .../site-packages/jaraco/context/__init__.py
        # site-packages is new_context_file.parent.parent.parent
        site_packages = new_context_file.parent.parent.parent

        src_dist_info = list(site_packages.glob(f"jaraco.context-{dist.version}.dist-info"))

        if not src_dist_info:
            print(f"Error: Could not locate source dist-info directory in {site_packages}")
            sys.exit(1)

        src_dist_info = src_dist_info[0]
        print(f"Source dist-info: {src_dist_info}")

        # Remove old dist-info from setuptools vendor
        old_dist_infos = list(vendor_path.glob("jaraco.context-*.dist-info"))
        for old in old_dist_infos:
            print(f"Removing old metadata: {old}")
            shutil.rmtree(old)

        # Copy new dist-info
        dest_dist_info = vendor_path / src_dist_info.name
        print(f"Copying metadata to {dest_dist_info}...")
        shutil.copytree(src_dist_info, dest_dist_info)

    except Exception as e:
        print(f"Error updating metadata: {e}")
        sys.exit(1)

    print("Successfully patched setuptools.")

if __name__ == "__main__":
    patch_setuptools()
