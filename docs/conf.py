"""Sphinx configuration for Sonix documentation.

See https://www.sphinx-doc.org/en/master/usage/configuration.html
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

# -- Project information -----------------------------------------------------

project = "Sonix"
copyright = "2026, Ryushin Wells"
author = "Ryushin Wells"

# -- General configuration ----------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# Generate anchors for h1-h3 so in-page links like [build order](#build-order)
# resolve in the Sphinx build the same way they do when Markdown is read
# directly on GitHub.
myst_heading_anchors = 3

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

autodoc_member_order = "bysource"
autodoc_typehints = "description"

# -- Options for HTML output --------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
