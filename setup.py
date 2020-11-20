#!/usr/bin/env python
"""The setup script."""

from setuptools import find_packages, setup

with open('README.md') as f:
    readme = f.read()

with open('CHANGELOG.md') as history_file:
    history = history_file.read()

install_requires = ['lxml', 'beautifulsoup4', 'click', 'requests']

extras = ["geopandas", "pandas", "shapely", "keplergl_cli"]
extra_reqs = {
    "docs": ["mkdocs", "mkdocs-material"],
    "cli": ["click", *extras],
    "extras": extras
}

# yapf: disable
setup(
    author="Kyle Barron",
    author_email='kylebarron2@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Experimental approach to deduce Sentinel 2 orbits",
    entry_points={
        'console_scripts': [
            's2-orbits=s2_orbit_geometry.cli:main',
        ],
    },
    install_requires=install_requires,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords=['sentinel', 'cogeo', 'geotiff'],
    name='s2_orbit_geometry',
    packages=find_packages(include=['s2_orbit_geometry', 's2_orbit_geometry.*']),
    setup_requires=[],
    extras_require=extra_reqs,
    test_suite='tests',
    tests_require=[],
    url='https://github.com/kylebarron/s2-orbit-geometry',
    version='0.1.0',
    zip_safe=False,
)
