from pathlib import Path
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


def parse_requirements_txt_to_list(filename):
    with open(filename, "r") as f:
        return f.read().splitlines()


setup(
    name="maestro_cli",
    version="0.1.0",
    license="GPL-3.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=parse_requirements_txt_to_list(
        str(Path(__file__).parent / "requirements.txt")),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    scripts=["bin/maestro"],
)
