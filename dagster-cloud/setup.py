from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open("dagster_cloud/version.py", encoding="utf8") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    return version["__version__"]


def get_description() -> str:
    return (Path(__file__).parent / "README.md").read_text()


if __name__ == "__main__":
    ver = get_version()
    # dont pin dev installs to avoid pip dep resolver issues
    pin = "" if ver == "0+dev" else f"=={ver}"
    setup(
        name="dagster_cloud",
        long_description=get_description(),
        long_description_content_type="text/markdown",
        version=ver,
        author_email="hello@elementl.com",
        packages=find_packages(exclude=["dagster_cloud_tests*"]),
        include_package_data=True,
        install_requires=[
            "dagster==1.0.13",
            "dagster-cloud-cli==1.0.13",
            "questionary",
            "requests",
            "typer[all]",
        ],
        extras_require={
            "tests": [
                "black",
                "docker",
                "httpretty",
                "isort",
                "kubernetes",
                "moto",
                "mypy",
                "paramiko",
                "pylint",
                "pytest",
                "types-PyYAML",
                "types-requests",
                "dagster_k8s==0.16.13",
            ],
            "docker": ["docker", "dagster_docker==0.16.13"],
            "kubernetes": ["kubernetes", "dagster_k8s==0.16.13"],
            "ecs": ["dagster_aws==0.16.13", "boto3"],
            "sandbox": ["supervisor"],
        },
        author="Elementl",
        license="Apache-2.0",
        classifiers=[
            "Programming Language :: Python :: 3.8",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
        ],
    )
