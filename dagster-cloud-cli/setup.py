from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open(Path(__file__).parent / "dagster_cloud_cli/version.py", encoding="utf8") as fp:
        exec(fp.read(), version)

    return version["__version__"]


ver = get_version()
# dont pin dev installs to avoid pip dep resolver issues
pin = "" if ver == "1!0+dev" else f"=={ver}"

setup(
    name="dagster-cloud-cli",
    version=get_version(),
    author_email="hello@elementl.com",
    packages=find_packages(exclude=["dagster_cloud.cli_tests*"]),
    include_package_data=True,
    install_requires=[
        "dagster==1.8.6",
        "packaging>=20.9",
        "questionary",
        "requests",
        "typer>=0.4.1",
        "PyYAML>=5.1",
        "github3.py",
    ],
    extras_require={
        "tests": [
            "freezegun",
        ],
    },
    author="Elementl",
    license="Apache-2.0",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "dagster-cloud = dagster_cloud_cli.entrypoint:app",
            "dagster-plus = dagster_cloud_cli.entrypoint:app",
        ]
    },
)
