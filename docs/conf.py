# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

# Warn about all references to unknown targets
nitpicky = True

# The master toctree document.
master_doc = 'index'

# -- Project information -----------------------------------------------------

project = 'tractor'
copyright = '2018, Tyler Goodlet'
author = 'Tyler Goodlet'

# The full version, including alpha/beta/rc tags
release = '0.0.0a0.dev0'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_book_theme'

pygments_style = 'algol_nu'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    # 'logo': 'tractor_logo_side.svg',
    # 'description': 'Structured concurrent "actors"',
    "repository_url": "https://github.com/goodboy/tractor",
    "use_repository_button": True,
    "home_page_in_toc": False,
    "show_toc_level": 1,
    "path_to_docs": "docs",

}
html_sidebars = {
    "**": [
        "sbt-sidebar-nav.html",
        "sidebar-search-bs.html",
        # 'localtoc.html',
    ],
    #     'logo.html',
    #     'github.html',
    #     'relations.html',
    #     'searchbox.html'
    # ]
}

# doesn't seem to work?
# extra_navbar = "<p>nextttt-gennnnn</p>"

html_title = ''
html_logo = '_static/tractor_logo_side.svg'
html_favicon = '_static/tractor_logo_side.svg'
# show_navbar_depth = 1

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pytest": ("https://docs.pytest.org/en/latest", None),
    "setuptools": ("https://setuptools.readthedocs.io/en/latest", None),
}
