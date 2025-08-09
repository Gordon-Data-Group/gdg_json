"""Package configuration for gdg_json."""

from setuptools import setup, find_packages

setup(
    name="gdg_json",
    version="0.0.2",
    packages=find_packages(),
    author="Gordon Data Group",
    author_email="grant@gordondatagroup.com",
    description="A simplified JSON parser",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Gordon-Data-Group/gdg_json",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
)
